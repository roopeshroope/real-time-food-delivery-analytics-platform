"""
Top-level orchestrator. Responsibilities:
  1. Build master data (zones/customers/restaurants/DPs) via WorldFactory
  2. Construct runtime state + all 5 generators, wiring their callbacks
  3. Run the Poisson order-arrival process (the "demand" driving everything else)
  4. Run background loops (restaurant heartbeat, DP supply pool, payment
     gateway outage watcher) as concurrent asyncio tasks
  5. Periodically log throughput stats
  6. Shut down cleanly after `simulation.duration_minutes` of *simulated*
     time, or on Ctrl+C
"""
from __future__ import annotations

import asyncio
import logging
import random

from simulator.clock import SimClock, peak_multiplier
from simulator.config import AppConfig
from simulator.entities import WorldFactory
from simulator.generators.dispatch_generator import DispatchGenerator
from simulator.generators.dp_generator import DPSupplyGenerator
from simulator.generators.gps_generator import TripRunner
from simulator.generators.order_generator import OrderLifecycleManager
from simulator.generators.payment_generator import PaymentGenerator
from simulator.generators.restaurant_generator import RestaurantGenerator
from simulator.kafka_producer import EventPublisher
from simulator.runtime_state import RuntimeState

log = logging.getLogger("engine")


class SimulatorEngine:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.rng = random.Random(cfg.simulation.random_seed)
        self.clock = SimClock(cfg.simulation)
        self.publisher = EventPublisher(cfg, self.rng)
        self.stop_event = asyncio.Event()

        world = WorldFactory(self.rng)
        zones = world.build_zones(cfg.scale.num_zones)
        customers = world.build_customers(cfg.scale.num_customers, zones)
        restaurants = world.build_restaurants(cfg.scale.num_restaurants, zones, cfg.restaurant)
        dps = world.build_delivery_partners(cfg.scale.num_delivery_partners, zones)

        self.state = RuntimeState(zones, customers, restaurants, dps, self.rng)

        self.order_manager = OrderLifecycleManager(cfg, self.state, self.clock, self.publisher, self.rng)
        self.payment_gen = PaymentGenerator(cfg, self.state, self.clock, self.publisher, self.rng)
        self.restaurant_gen = RestaurantGenerator(cfg, self.state, self.clock, self.publisher, self.rng)
        self.dp_supply_gen = DPSupplyGenerator(cfg, self.state, self.clock, self.publisher, self.rng)
        self.trip_runner = TripRunner(cfg, self.state, self.clock, self.publisher, self.rng, self.order_manager)
        self.dispatch_gen = DispatchGenerator(cfg, self.state, self.clock, self.publisher, self.rng,
                                               self.order_manager, self.trip_runner)

        # wire cross-generator callbacks (kept as plain callables to avoid
        # circular imports between order_generator <-> dispatch/payment)
        self.order_manager.on_order_ready = self.dispatch_gen.assign
        self.order_manager.on_payment_needed = self.payment_gen.charge

        log.info(
            "World built: %d zones, %d customers, %d restaurants, %d DPs",
            len(zones), len(customers), len(restaurants), len(dps),
        )

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self._order_arrival_loop(), name="order_arrival"),
            asyncio.create_task(self.restaurant_gen.run(self.stop_event), name="restaurant_gen"),
            asyncio.create_task(self.dp_supply_gen.run(self.stop_event), name="dp_supply"),
            asyncio.create_task(self.payment_gen.run_outage_watcher(self.stop_event), name="payment_outage"),
            asyncio.create_task(self._stats_loop(), name="stats"),
            asyncio.create_task(self._duration_watchdog(), name="watchdog"),
        ]
        try:
            await self.stop_event.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.publisher.close()

    async def _duration_watchdog(self) -> None:
        real_seconds = self.clock.real_seconds_for(self.cfg.simulation.duration_minutes)
        await asyncio.sleep(real_seconds)
        log.info("Configured duration (%.1f simulated minutes) elapsed, stopping.",
                  self.cfg.simulation.duration_minutes)
        self.stop_event.set()

    async def _stats_loop(self) -> None:
        while not self.stop_event.is_set():
            await asyncio.sleep(15)
            stats = self.publisher.stats()
            log.info(
                "stats: sim_time=%s active_orders=%d online_dps=%d published=%d errors=%d",
                self.clock.now().isoformat(timespec="seconds"),
                len(self.state.active_orders),
                sum(1 for d in self.state.dps.values() if d.is_online),
                stats["sent"], stats["errors"],
            )

    async def _order_arrival_loop(self) -> None:
        zone_ids = list(self.state.zones.keys())
        zone_weights = [self.state.zones[z].demand_weight for z in zone_ids]

        while not self.stop_event.is_set():
            hour = self.clock.sim_hour_of_day()
            mult, label = peak_multiplier(hour, self.cfg.demand)
            rate_per_min = self.cfg.demand.base_orders_per_min * mult
            rate_per_min = max(0.05, rate_per_min)

            # Poisson process: inter-arrival time ~ Exponential(rate)
            interarrival_min = self.rng.expovariate(rate_per_min)
            await asyncio.sleep(self.clock.real_seconds_for(interarrival_min))

            if self.stop_event.is_set():
                break

            zone_id = self.rng.choices(zone_ids, weights=zone_weights, k=1)[0]
            try:
                await self.order_manager.spawn_order(zone_id)
            except Exception:
                log.exception("Failed to spawn order in zone %s", zone_id)
