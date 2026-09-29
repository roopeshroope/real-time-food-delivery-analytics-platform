"""
Emits restaurant-status-events:
  - random ONLINE/OFFLINE toggling (staff shortage, closing early, etc.)
  - periodic backlog/busy snapshots derived from current_backlog vs capacity

Backlog itself is mutated by order_generator.py (increment on ACCEPTED,
decrement on READY/CANCELLED) -- this generator just periodically *observes*
and publishes that state, exactly like a real POS heartbeat would.
"""
from __future__ import annotations

import asyncio
import logging
import random

from simulator.clock import SimClock
from simulator.config import AppConfig
from simulator.entities import Restaurant
from simulator.kafka_producer import EventPublisher
from simulator.runtime_state import RuntimeState
from simulator.schemas import RestaurantStatusEvent, iso_now
from simulator.state_machines import RestaurantStatus

log = logging.getLogger("gen.restaurant")


class RestaurantGenerator:
    def __init__(self, cfg: AppConfig, state: RuntimeState, clock: SimClock,
                 publisher: EventPublisher, rng: random.Random):
        self.cfg = cfg
        self.state = state
        self.clock = clock
        self.publisher = publisher
        self.rng = rng

    async def run(self, stop_event: asyncio.Event) -> None:
        """Periodic heartbeat loop: every ~2 simulated minutes, sweep all
        restaurants for offline toggles + emit a backlog snapshot."""
        heartbeat_sim_minutes = 2.0
        while not stop_event.is_set():
            for restaurant in list(self.state.restaurants.values()):
                await self._maybe_toggle_offline(restaurant, heartbeat_sim_minutes)
                await self._emit_backlog_snapshot(restaurant)
            await asyncio.sleep(self.clock.real_seconds_for(heartbeat_sim_minutes))

    async def _maybe_toggle_offline(self, restaurant: Restaurant, window_minutes: float) -> None:
        p = self.cfg.restaurant.offline_probability_per_hour * (window_minutes / 60.0)
        if restaurant.is_online and self.rng.random() < p:
            restaurant.is_online = False
            await self._emit(restaurant, RestaurantStatus.OFFLINE, reason="staff_shortage")
            log.info("restaurant %s went OFFLINE", restaurant.restaurant_id)
        elif not restaurant.is_online and self.rng.random() < 0.3:
            restaurant.is_online = True
            await self._emit(restaurant, RestaurantStatus.ONLINE, reason="reopened")
            log.info("restaurant %s back ONLINE", restaurant.restaurant_id)

    async def _emit_backlog_snapshot(self, restaurant: Restaurant) -> None:
        status = RestaurantStatus.BUSY if restaurant.current_backlog >= restaurant.capacity_orders \
            else RestaurantStatus.NORMAL
        await self._emit(restaurant, status, reason=None)

    async def _emit(self, restaurant: Restaurant, status: str, reason: str | None) -> None:
        event = RestaurantStatusEvent(
            event_time=iso_now(self.clock.now()),
            restaurant_id=restaurant.restaurant_id,
            zone_id=restaurant.zone_id,
            status=status,
            current_backlog=restaurant.current_backlog,
            capacity_orders=restaurant.capacity_orders,
            avg_prep_minutes_observed=round(restaurant.avg_prep_minutes, 1),
            reason=reason,
        )
        await self.publisher.publish("restaurant_status", key=restaurant.restaurant_id, event=event)
