"""
Master-data generation. These are the "dimension" entities the event streams
reference by id. Generated once at simulator startup and held in memory for
the run (a real system would source these from an OLTP DB / CDC stream --
out of scope here, this simulator focuses purely on the event layer).
"""
from __future__ import annotations

import dataclasses
import random
import uuid
from typing import Optional

from simulator.geo import LatLon, random_point_in_radius, cell_id


# Rough Hyderabad-centered bounding box, purely illustrative
CITY_CENTER = LatLon(17.3850, 78.4867)
CITY_RADIUS_KM = 14.0


@dataclasses.dataclass
class Zone:
    zone_id: str
    name: str
    center: LatLon
    radius_km: float
    demand_weight: float  # relative baseline popularity


@dataclasses.dataclass
class Customer:
    customer_id: str
    zone_id: str
    home: LatLon
    signup_days_ago: int


@dataclasses.dataclass
class Restaurant:
    restaurant_id: str
    name: str
    zone_id: str
    location: LatLon
    cuisine: str
    avg_prep_minutes: float
    capacity_orders: int
    rating: float
    is_online: bool = True
    current_backlog: int = 0


@dataclasses.dataclass
class DeliveryPartner:
    dp_id: str
    name: str
    home_zone_id: str
    vehicle_type: str
    rating: float
    is_online: bool = False
    status: str = "OFFLINE"          # mirrors DPStateMachine states
    current_location: Optional[LatLon] = None
    active_order_ids: list = dataclasses.field(default_factory=list)


CUISINES = [
    "North Indian", "South Indian", "Chinese", "Biryani", "Fast Food",
    "Bakery", "Desserts", "Pizza", "Beverages", "Street Food", "Healthy",
]
VEHICLE_TYPES = ["bike", "scooter", "bicycle"]
ZONE_NAME_POOL = [
    "Gachibowli", "Madhapur", "Kondapur", "Banjara Hills", "Secunderabad",
    "Kukatpally", "Ameerpet", "Begumpet", "Hitech City", "LB Nagar",
]


class WorldFactory:
    """Builds the full master-data set for a simulation run."""

    def __init__(self, rng: random.Random):
        self.rng = rng

    def build_zones(self, n: int) -> list[Zone]:
        zones = []
        names = self.rng.sample(ZONE_NAME_POOL, k=min(n, len(ZONE_NAME_POOL)))
        while len(names) < n:
            names.append(f"Zone-{len(names)+1}")

        for i in range(n):
            center = random_point_in_radius(CITY_CENTER, CITY_RADIUS_KM * 0.7, self.rng)
            zones.append(
                Zone(
                    zone_id=f"Z{i+1:02d}",
                    name=names[i],
                    center=center,
                    radius_km=self.rng.uniform(1.8, 3.5),
                    demand_weight=self.rng.uniform(0.5, 2.2),
                )
            )
        return zones

    def build_customers(self, n: int, zones: list[Zone]) -> list[Customer]:
        customers = []
        weights = [z.demand_weight for z in zones]
        for i in range(n):
            zone = self.rng.choices(zones, weights=weights, k=1)[0]
            home = random_point_in_radius(zone.center, zone.radius_km, self.rng)
            customers.append(
                Customer(
                    customer_id=f"CUST-{uuid.uuid4().hex[:10]}",
                    zone_id=zone.zone_id,
                    home=home,
                    signup_days_ago=self.rng.randint(1, 900),
                )
            )
        return customers

    def build_restaurants(self, n: int, zones: list[Zone], cfg) -> list[Restaurant]:
        restaurants = []
        weights = [z.demand_weight for z in zones]
        for i in range(n):
            zone = self.rng.choices(zones, weights=weights, k=1)[0]
            loc = random_point_in_radius(zone.center, zone.radius_km, self.rng)
            lo, hi = cfg.avg_prep_minutes_range
            cap_lo, cap_hi = cfg.capacity_orders_range
            restaurants.append(
                Restaurant(
                    restaurant_id=f"REST-{uuid.uuid4().hex[:10]}",
                    name=f"{self.rng.choice(CUISINES)} Kitchen #{i+1}",
                    zone_id=zone.zone_id,
                    location=loc,
                    cuisine=self.rng.choice(CUISINES),
                    avg_prep_minutes=self.rng.uniform(lo, hi),
                    capacity_orders=self.rng.randint(cap_lo, cap_hi),
                    rating=round(self.rng.uniform(3.2, 4.9), 1),
                )
            )
        return restaurants

    def build_delivery_partners(self, n: int, zones: list[Zone]) -> list[DeliveryPartner]:
        dps = []
        weights = [z.demand_weight for z in zones]
        for i in range(n):
            zone = self.rng.choices(zones, weights=weights, k=1)[0]
            dps.append(
                DeliveryPartner(
                    dp_id=f"DP-{uuid.uuid4().hex[:10]}",
                    name=f"Partner-{i+1}",
                    home_zone_id=zone.zone_id,
                    vehicle_type=self.rng.choice(VEHICLE_TYPES),
                    rating=round(self.rng.uniform(3.5, 5.0), 1),
                )
            )
        return dps


def zone_cell(zone: Zone) -> str:
    return cell_id(zone.center)
