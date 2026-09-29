"""
Simulated clock. Real wall-clock time advances the simulated clock at
`speed_factor` x -- e.g. speed_factor=30 means 30 simulated minutes pass
per 1 real minute (1 simulated second per ~33ms real time... in practice we
reason in minutes since that's the unit configs are expressed in).

All generators ask this clock "what simulated time is it right now" and
"how many *real* seconds should I sleep to represent N simulated minutes"
rather than touching wall clock arithmetic themselves.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from simulator.config import SimulationConfig, DemandConfig


class SimClock:
    def __init__(self, sim_cfg: SimulationConfig):
        self.speed_factor = sim_cfg.speed_factor
        self._real_start = time.monotonic()
        today = datetime.now(timezone.utc).date()
        start_hour = sim_cfg.start_sim_hour
        self._sim_start = datetime(
            today.year, today.month, today.day, tzinfo=timezone.utc
        ) + timedelta(hours=start_hour)

    def now(self) -> datetime:
        real_elapsed_sec = time.monotonic() - self._real_start
        sim_elapsed_sec = real_elapsed_sec * self.speed_factor
        return self._sim_start + timedelta(seconds=sim_elapsed_sec)

    def sim_hour_of_day(self) -> float:
        n = self.now()
        return n.hour + n.minute / 60.0 + n.second / 3600.0

    def real_seconds_for(self, sim_minutes: float) -> float:
        """Real seconds to sleep to represent `sim_minutes` of simulated time."""
        sim_seconds = sim_minutes * 60.0
        return max(0.0, sim_seconds / self.speed_factor)

    def real_seconds_for_sim_seconds(self, sim_seconds: float) -> float:
        return max(0.0, sim_seconds / self.speed_factor)


def peak_multiplier(hour: float, demand_cfg: DemandConfig) -> tuple[float, str]:
    """Returns (multiplier, label) for the current simulated hour-of-day."""
    for w in demand_cfg.peak_windows:
        if w.start <= hour <= w.end:
            return w.multiplier, w.label
    return demand_cfg.off_peak_multiplier, "off_peak"
