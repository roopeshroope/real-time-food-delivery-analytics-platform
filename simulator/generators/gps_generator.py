"""
Owns gps-pings AND the per-trip leg of dp-status-events (ASSIGNED ->
EN_ROUTE_TO_RESTAURANT -> WAITING_AT_RESTAURANT -> PICKED_UP ->
EN_ROUTE_TO_CUSTOMER -> DELIVERED -> IDLE). GPS position and DP trip-state
are inherently the same physical journey, so modelling them together avoids
two generators fighting over "where is this DP right now".

Also handles batched multi-dropoff trips: dispatch_generator.py can attach a
second order to a DP that's still WAITING_AT_RESTAURANT at the same
restaurant (see runtime_state.pending_batch_dropoffs); this module then
sequences both dropoffs in a single EN_ROUTE_TO_CUSTOMER leg.
"""
from __future__ import annotations

import asyncio
import logging
import random

from simulator.chaos import maybe_gps_anomaly, maybe_gps_dropout
from simulator.clock import SimClock
from simulator.config import AppConfig
from simulator.entities import DeliveryPartner
from simulator.geo import LatLon, haversine_km, interpolate_route, bearing_degrees, random_point_in_radius
from simulator.kafka_producer import EventPublisher
from simulator.runtime_state import RuntimeState
from simulator.schemas import DPStatusEvent, GPSPingEvent, iso_now
from simulator.state_machines import DPStatus

log = logging.getLogger("gen.gps")


class TripRunner:
    def __init__(self, cfg: AppConfig, state: RuntimeState, clock: SimClock,
                 publisher: EventPublisher, rng: random.Random, order_manager):
        self.cfg = cfg
        self.state = state
        self.clock = clock
        self.publisher = publisher
        self.rng = rng
        self.order_manager = order_manager  # OrderLifecycleManager, for transition() calls

    async def run(self, dp_id: str, primary_order_id: str) -> None:
        try:
            dp = self.state.dps[dp_id]

            await self._set_dp_status(dp, DPStatus.EN_ROUTE_TO_RESTAURANT, order_id=primary_order_id)
            order = self.state.active_orders.get(primary_order_id)
            if order is None:
                return  # cancelled before DP started moving
            restaurant_loc = order.pickup_location

            await self._drive_leg(dp, dp.current_location, restaurant_loc, trip_leg="TO_RESTAURANT",
                                   order_id=primary_order_id)
            dp.current_location = restaurant_loc

            await self._set_dp_status(dp, DPStatus.WAITING_AT_RESTAURANT, order_id=primary_order_id)

            # -- allow a short batching window: dispatch_generator may attach
            #    a second order to this DP while it's waiting at the restaurant
            await asyncio.sleep(self.clock.real_seconds_for_sim_seconds(self.rng.uniform(5, 20)))

            pending_orders = [primary_order_id] + self.state.pending_batch_dropoffs.pop(dp_id, [])
            valid_orders = []
            for oid in pending_orders:
                o = self.state.active_orders.get(oid)
                if o is not None:
                    valid_orders.append(o)

            if not valid_orders:
                await self._release_dp(dp)
                return

            # wait for every batched order to actually reach READY before pickup
            for o in valid_orders:
                for _ in range(20):
                    if o.state in ("READY", "ASSIGNED"):
                        break
                    await asyncio.sleep(self.clock.real_seconds_for_sim_seconds(2))

            for o in valid_orders:
                await self.order_manager.transition(o.order_id, "PICKED_UP")

            await self._set_dp_status(dp, DPStatus.PICKED_UP, order_id=primary_order_id)
            await self._set_dp_status(dp, DPStatus.EN_ROUTE_TO_CUSTOMER, order_id=primary_order_id)

            # -- sequence dropoffs nearest-first from the restaurant --
            remaining = list(valid_orders)
            current_loc = restaurant_loc
            remaining.sort(key=lambda o: haversine_km(current_loc, o.dropoff_location))

            for o in remaining:
                await self._drive_leg(dp, current_loc, o.dropoff_location, trip_leg="TO_CUSTOMER",
                                       order_id=o.order_id)
                current_loc = o.dropoff_location
                dp.current_location = current_loc
                await self.order_manager.transition(o.order_id, "DELIVERED")

            await self._set_dp_status(dp, DPStatus.DELIVERED, order_id=primary_order_id)
            await self._release_dp(dp)

        except Exception:
            log.exception("TripRunner failed for dp_id=%s order_id=%s", dp_id, primary_order_id)
            dp = self.state.dps.get(dp_id)
            if dp:
                await self._release_dp(dp)

    async def _release_dp(self, dp: DeliveryPartner) -> None:
        dp.active_order_ids = []
        dp.status = DPStatus.IDLE
        self.state.mark_dp_idle(dp.dp_id)
        await self._set_dp_status(dp, DPStatus.IDLE, order_id=None, _already_set=True)

    # ------------------------------------------------------------------
    async def _drive_leg(self, dp: DeliveryPartner, start: LatLon, end: LatLon,
                          trip_leg: str, order_id: str) -> None:
        scfg = self.cfg.supply
        dcfg = self.cfg.dispatch
        distance_km = max(0.05, haversine_km(start, end))

        base_speed = self.rng.uniform(*scfg.dp_speed_kmph_range)
        in_traffic = self.rng.random() < scfg.traffic_slowdown_probability
        effective_speed = base_speed * (scfg.traffic_slowdown_factor if in_traffic else 1.0)

        travel_hours = distance_km / effective_speed
        travel_minutes = travel_hours * 60.0
        n_steps = max(3, int((travel_minutes * 60) / dcfg.gps_ping_interval_seconds))

        skip_remaining = 0
        for i in range(1, n_steps + 1):
            fraction = i / n_steps
            point = interpolate_route(start, end, fraction, jitter_km=0.08, rng=self.rng)
            heading = bearing_degrees(start, end)

            anomaly = maybe_gps_anomaly(self.rng, self.cfg.chaos.gps_anomaly_probability)
            if anomaly.anomaly_type == "teleport":
                point = random_point_in_radius(point, self.rng.uniform(0.8, 2.5), self.rng)
            elif anomaly.anomaly_type == "stationary_drift":
                point = start  # DP appears stuck despite an active trip

            if skip_remaining > 0:
                skip_remaining -= 1
            else:
                skip_remaining = maybe_gps_dropout(
                    self.rng, self.cfg.chaos.gps_dropout_probability, self.cfg.chaos.gps_dropout_pings
                )
                await self._emit_ping(dp, order_id, point, effective_speed, heading, trip_leg,
                                       anomaly.anomaly_type)

            dp.current_location = point
            await asyncio.sleep(self.clock.real_seconds_for_sim_seconds(dcfg.gps_ping_interval_seconds))

    async def _emit_ping(self, dp: DeliveryPartner, order_id: str, point: LatLon, speed: float,
                          heading: float, trip_leg: str, anomaly_type: str | None) -> None:
        event = GPSPingEvent(
            event_time=iso_now(self.clock.now()),
            dp_id=dp.dp_id,
            order_id=order_id,
            lat=round(point.lat, 6),
            lon=round(point.lon, 6),
            speed_kmph=round(speed, 1),
            heading_degrees=round(heading, 1),
            accuracy_meters=round(self.rng.uniform(4, 15), 1),
            trip_leg=trip_leg,
            debug_anomaly_type=anomaly_type,
        )
        await self.publisher.publish("gps_pings", key=dp.dp_id, event=event)

    async def _set_dp_status(self, dp: DeliveryPartner, status: str, order_id: str | None,
                              _already_set: bool = False) -> None:
        previous = dp.status
        if not _already_set:
            dp.status = status
        event = DPStatusEvent(
            event_time=iso_now(self.clock.now()),
            dp_id=dp.dp_id,
            zone_id=dp.home_zone_id,
            status=status,
            previous_status=previous,
            order_id=order_id,
            lat=dp.current_location.lat if dp.current_location else None,
            lon=dp.current_location.lon if dp.current_location else None,
        )
        await self.publisher.publish("dp_status", key=dp.dp_id, event=event)
