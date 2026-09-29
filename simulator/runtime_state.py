"""
Shared, in-memory runtime state that generators read/mutate as the
simulation progresses. This is the simulator's stand-in for what would, in a
real system, be spread across an OLTP DB + a dispatch service's in-memory
matching state. Everything here is asyncio-single-threaded (we don't spawn
OS threads for simulation logic, only kafka-python's internal IO thread
touches this from outside the event loop, and it never touches this class),
so no locking is required across generators.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Optional

from simulator.entities import Zone, Customer, Restaurant, DeliveryPartner
from simulator.geo import LatLon


@dataclass
class ActiveOrder:
    order_id: str
    customer_id: str
    restaurant_id: str
    zone_id: str
    state: str
    promised_eta_ts: str
    predicted_ready_ts: Optional[str] = None
    dp_id: Optional[str] = None
    batch_id: Optional[str] = None
    pickup_location: LatLon = None
    dropoff_location: LatLon = None


class RuntimeState:
    def __init__(self, zones: list[Zone], customers: list[Customer],
                 restaurants: list[Restaurant], dps: list[DeliveryPartner],
                 rng: random.Random):
        self.rng = rng
        self.zones: dict[str, Zone] = {z.zone_id: z for z in zones}
        self.customers: dict[str, Customer] = {c.customer_id: c for c in customers}
        self.restaurants: dict[str, Restaurant] = {r.restaurant_id: r for r in restaurants}
        self.dps: dict[str, DeliveryPartner] = {d.dp_id: d for d in dps}

        # zone_id -> list[dp_id] currently IDLE and available for assignment
        self.idle_dps_by_zone: dict[str, set] = {z.zone_id: set() for z in zones}
        # zone_id -> list[restaurant_id]
        self.restaurants_by_zone: dict[str, list] = {z.zone_id: [] for z in zones}
        for r in restaurants:
            self.restaurants_by_zone[r.zone_id].append(r.restaurant_id)

        self.active_orders: dict[str, ActiveOrder] = {}
        self.pending_ready_orders_by_zone: dict[str, set] = {z.zone_id: set() for z in zones}

        # dp_id -> list[order_id] batched onto a DP that's currently
        # WAITING_AT_RESTAURANT for its primary order (see dispatch_generator
        # and gps_generator.TripRunner)
        self.pending_batch_dropoffs: dict[str, list] = {}

        # payment gateway pool for round-robin / weighted selection
        self.gateways = ["Razorpay", "PayU", "CCAvenue", "UPI-Direct"]
        self.payment_methods = ["UPI", "CARD", "NETBANKING", "WALLET", "COD"]

    def customers_in_zone(self, zone_id: str) -> list[str]:
        return [c.customer_id for c in self.customers.values() if c.zone_id == zone_id]

    def restaurant_for_order(self, zone_id: str) -> Optional[str]:
        candidates = [
            rid for rid in self.restaurants_by_zone[zone_id]
            if self.restaurants[rid].is_online
        ]
        if not candidates:
            return None
        # weight by inverse backlog (less busy restaurants get more orders) + rating
        weights = [
            max(0.05, self.restaurants[rid].rating - 0.5 * self.restaurants[rid].current_backlog)
            for rid in candidates
        ]
        return self.rng.choices(candidates, weights=weights, k=1)[0]

    def mark_dp_idle(self, dp_id: str) -> None:
        dp = self.dps[dp_id]
        self.idle_dps_by_zone[dp.home_zone_id].add(dp_id)

    def unmark_dp_idle(self, dp_id: str) -> None:
        dp = self.dps[dp_id]
        self.idle_dps_by_zone[dp.home_zone_id].discard(dp_id)

    def nearest_idle_dps(self, zone_id: str, origin: LatLon, k: int) -> list[str]:
        from simulator.geo import haversine_km
        pool = list(self.idle_dps_by_zone.get(zone_id, set()))
        pool.sort(key=lambda dp_id: haversine_km(origin, self.dps[dp_id].current_location or origin))
        return pool[:k]
