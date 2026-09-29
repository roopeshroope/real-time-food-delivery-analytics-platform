"""
Realistic-failure injection that doesn't fit neatly into a single generator:
- GPS anomaly / dropout decisions (consumed by generators/gps_generator.py)
- A shared payment-gateway "outage window" tracker so multiple concurrent
  payment attempts during an outage are correlated (a retry storm), instead
  of every failure being an independent coin flip.

Duplicate/late/out-of-order EVENT PUBLISHING chaos lives in kafka_producer.py
since it applies uniformly at the publish boundary for every topic.
"""
from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from typing import Optional

from simulator.config import PaymentConfig


@dataclass
class GPSAnomalyDecision:
    anomaly_type: Optional[str] = None   # None | "teleport" | "stationary_drift"


def maybe_gps_anomaly(rng: random.Random, probability: float) -> GPSAnomalyDecision:
    if rng.random() < probability:
        anomaly_type = rng.choice(["teleport", "stationary_drift"])
        return GPSAnomalyDecision(anomaly_type=anomaly_type)
    return GPSAnomalyDecision()


def maybe_gps_dropout(rng: random.Random, probability: float, dropout_range: list) -> int:
    """Returns number of consecutive pings to *skip* (0 if no dropout)."""
    if rng.random() < probability:
        lo, hi = dropout_range
        return rng.randint(lo, hi)
    return 0


class GatewayOutageTracker:
    """
    Thread-safe tracker for simulated payment-gateway outage windows.
    A background-free, poll-based design: `maybe_start_outage()` is checked
    periodically by the payment generator's own loop (per configured hourly
    probability), and `is_outage_active()` is checked per payment attempt.
    """

    def __init__(self, cfg: PaymentConfig, rng: random.Random, sim_clock):
        self.cfg = cfg
        self.rng = rng
        self.sim_clock = sim_clock
        self._lock = threading.Lock()
        self._outage_until: Optional[float] = None  # monotonic real seconds

    def maybe_start_outage(self, real_seconds_elapsed_hint: float) -> None:
        with self._lock:
            if self._outage_until is not None:
                return  # already in an outage
            # convert hourly probability into a per-check probability using
            # the caller's polling interval
            p = self.cfg.gateway_outage_probability_per_hour * (real_seconds_elapsed_hint / 3600.0)
            if self.rng.random() < p:
                lo, hi = self.cfg.gateway_outage_duration_minutes_range
                duration_sim_min = self.rng.uniform(lo, hi)
                duration_real_sec = self.sim_clock.real_seconds_for(duration_sim_min)
                self._outage_until = time.monotonic() + duration_real_sec

    def is_outage_active(self) -> bool:
        with self._lock:
            if self._outage_until is None:
                return False
            if time.monotonic() >= self._outage_until:
                self._outage_until = None
                return False
            return True
