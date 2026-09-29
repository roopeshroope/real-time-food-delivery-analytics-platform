"""
Manages the DP *supply pool*: how many delivery partners are online per
zone right now. Target online-ratio follows the same peak/off-peak curve as
demand (drivers log on around lunch/dinner) but is deliberately capped below
100%, and can dip further via `supply.shortage_probability`, to create
genuine supply-demand imbalance for the liquidity metrics to detect.

Per-trip DP status transitions (ASSIGNED -> ... -> DELIVERED) are owned by
gps_generator.py's TripRunner, since they're driven by GPS progress, not by
this supply-pool loop.
"""
from __future__ import annotations

import asyncio
import logging
import random

from simulator.clock import SimClock, peak_multiplier
from simulator.config import AppConfig
from simulator.entities import DeliveryPartner
from simulator.geo import random_point_in_radius
from simulator.kafka_producer import EventPublisher
from simulator.runtime_state import RuntimeState
from simulator.schemas import DPStatusEvent, iso_now
from simulator.state_machines import DPStatus

log = logging.getLogger("gen.dp")


class DPSupplyGenerator:
    def __init__(self, cfg: AppConfig, state: RuntimeState, clock: SimClock,
                 publisher: EventPublisher, rng: random.Random):
        self.cfg = cfg
        self.state = state
        self.clock = clock
        self.publisher = publisher
        self.rng = rng
        self._shortage_zones: set[str] = set()

    async def run(self, stop_event: asyncio.Event) -> None:
        sweep_sim_minutes = 3.0
        while not stop_event.is_set():
            self._maybe_toggle_shortages()
            hour = self.clock.sim_hour_of_day()
            mult, _label = peak_multiplier(hour, self.cfg.demand)
            is_peak = mult > self.cfg.demand.off_peak_multiplier

            base_ratio = (self.cfg.supply.peak_dp_online_ratio if is_peak
                          else self.cfg.supply.base_dp_online_ratio)

            for zone_id in self.state.zones:
                target_ratio = base_ratio
                if zone_id in self._shortage_zones:
                    target_ratio *= self.cfg.supply.shortage_multiplier
                await self._rebalance_zone(zone_id, target_ratio)

            await asyncio.sleep(self.clock.real_seconds_for(sweep_sim_minutes))

    def _maybe_toggle_shortages(self) -> None:
        for zone_id in self.state.zones:
            if zone_id in self._shortage_zones:
                if self.rng.random() < 0.25:  # shortage resolves
                    self._shortage_zones.discard(zone_id)
                    log.info("zone %s supply shortage resolved", zone_id)
            else:
                if self.rng.random() < self.cfg.supply.shortage_probability * 0.1:
                    self._shortage_zones.add(zone_id)
                    log.info("zone %s entering supply shortage", zone_id)

    async def _rebalance_zone(self, zone_id: str, target_ratio: float) -> None:
        zone_dps = [d for d in self.state.dps.values() if d.home_zone_id == zone_id]
        if not zone_dps:
            return
        target_online = int(round(len(zone_dps) * target_ratio))

        online_dps = [d for d in zone_dps if d.is_online]
        offline_dps = [d for d in zone_dps if not d.is_online]

        # bring more DPs online
        deficit = target_online - len(online_dps)
        if deficit > 0:
            for dp in self.rng.sample(offline_dps, k=min(deficit, len(offline_dps))):
                await self._go_online(dp, zone_id)

        # take idle (not on-trip) DPs offline if we're over target
        surplus = len(online_dps) - target_online
        if surplus > 0:
            idle_candidates = [d for d in online_dps if d.status == DPStatus.IDLE]
            for dp in self.rng.sample(idle_candidates, k=min(surplus, len(idle_candidates))):
                await self._go_offline(dp, zone_id)

    async def _go_online(self, dp: DeliveryPartner, zone_id: str) -> None:
        zone = self.state.zones[zone_id]
        dp.current_location = random_point_in_radius(zone.center, zone.radius_km, self.rng)
        dp.is_online = True
        previous = dp.status
        dp.status = DPStatus.IDLE
        self.state.mark_dp_idle(dp.dp_id)
        await self._emit(dp, previous)

    async def _go_offline(self, dp: DeliveryPartner, zone_id: str) -> None:
        previous = dp.status
        dp.is_online = False
        dp.status = DPStatus.OFFLINE
        self.state.unmark_dp_idle(dp.dp_id)
        await self._emit(dp, previous)

    async def _emit(self, dp: DeliveryPartner, previous_status: str, order_id: str | None = None) -> None:
        event = DPStatusEvent(
            event_time=iso_now(self.clock.now()),
            dp_id=dp.dp_id,
            zone_id=dp.home_zone_id,
            status=dp.status,
            previous_status=previous_status,
            order_id=order_id,
            lat=dp.current_location.lat if dp.current_location else None,
            lon=dp.current_location.lon if dp.current_location else None,
        )
        await self.publisher.publish("dp_status", key=dp.dp_id, event=event)
