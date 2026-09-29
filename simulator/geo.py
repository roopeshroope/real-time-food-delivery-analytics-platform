"""
Geo utilities: distance calculation, simple zone-cell hashing, and route
interpolation used to generate realistic GPS ping trajectories.

We avoid an external geohash dependency to keep the simulator dependency-light;
`cell_id()` below produces a stable, geohash-like string sufficient for
zone-level joins downstream (partition/group-by key in the streaming layer).
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass


EARTH_RADIUS_KM = 6371.0


@dataclass(frozen=True)
class LatLon:
    lat: float
    lon: float


def haversine_km(a: LatLon, b: LatLon) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, [a.lat, a.lon, b.lat, b.lon])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(h)))


def cell_id(point: LatLon, precision: int = 6) -> str:
    """
    Cheap geohash-like grid cell id. Not a real geohash, but deterministic,
    stable and locality-preserving enough for zone-level streaming aggregation
    (equivalent role to a geohash-6 / H3-res-8 cell key in this simulator).
    """
    lat_bucket = int((point.lat + 90.0) * 10 ** (precision - 2))
    lon_bucket = int((point.lon + 180.0) * 10 ** (precision - 2))
    return f"gh{lat_bucket:x}{lon_bucket:x}"[:10]


def random_point_in_radius(center: LatLon, radius_km: float, rng: random.Random) -> LatLon:
    """Uniform-ish random point within radius_km of center (small-angle approx)."""
    r = radius_km * math.sqrt(rng.random())
    theta = rng.random() * 2 * math.pi
    dlat = (r / EARTH_RADIUS_KM) * (180 / math.pi) * math.cos(theta)
    dlon = (
        (r / EARTH_RADIUS_KM)
        * (180 / math.pi)
        * math.sin(theta)
        / math.cos(math.radians(center.lat))
    )
    return LatLon(center.lat + dlat, center.lon + dlon)


def interpolate_route(start: LatLon, end: LatLon, fraction: float, jitter_km: float = 0.0,
                       rng: random.Random | None = None) -> LatLon:
    """
    Linear interpolation between start/end with optional small perpendicular
    jitter to emulate road-network wiggle instead of a perfectly straight line.
    fraction in [0, 1].
    """
    fraction = max(0.0, min(1.0, fraction))
    lat = start.lat + (end.lat - start.lat) * fraction
    lon = start.lon + (end.lon - start.lon) * fraction

    if jitter_km > 0 and rng is not None:
        # perpendicular offset, tapering to zero at both endpoints so the
        # route still visually starts/ends exactly on the two points
        taper = math.sin(fraction * math.pi)
        offset = rng.uniform(-jitter_km, jitter_km) * taper
        bearing = math.atan2(end.lon - start.lon, end.lat - start.lat) + math.pi / 2
        dlat = (offset / EARTH_RADIUS_KM) * (180 / math.pi) * math.cos(bearing)
        dlon = (
            (offset / EARTH_RADIUS_KM)
            * (180 / math.pi)
            * math.sin(bearing)
            / math.cos(math.radians(lat))
        )
        lat += dlat
        lon += dlon

    return LatLon(lat, lon)


def bearing_degrees(start: LatLon, end: LatLon) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, [start.lat, start.lon, end.lat, end.lon])
    dlon = lon2 - lon1
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360
