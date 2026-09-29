"""
Loads config/config.yaml into typed, validated dataclasses.
Every tunable in the simulator flows through this module -- nothing should be
hardcoded deeper in the codebase.
"""
from __future__ import annotations

import dataclasses
import os
import re
from pathlib import Path
from typing import Any, Optional

import yaml

# Matches ${VAR}, ${VAR:-default}, and $VAR (bash-style default-value syntax).
# This lets config.yaml stay committable while real values (Confluent Cloud
# API keys, bootstrap servers, etc.) come from the environment or a .env file.
_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}|\$([A-Za-z_][A-Za-z0-9_]*)")


def _expand_env_vars(text: str) -> str:
    def _sub(match: "re.Match") -> str:
        braced_name, _, default, bare_name = match.groups()
        name = braced_name or bare_name
        return os.environ.get(name, default if default is not None else "")

    return _ENV_VAR_PATTERN.sub(_sub, text)


@dataclasses.dataclass
class PeakWindow:
    start: float
    end: float
    multiplier: float
    label: str


@dataclasses.dataclass
class OutputConfig:
    mode: str
    file_dir: str


@dataclasses.dataclass
class KafkaConfig:
    bootstrap_servers: str
    topics: dict
    acks: str
    linger_ms: int
    retries: int
    compression_type: str
    # Confluent Cloud / any SASL_SSL cluster. Defaults keep a local
    # docker-compose broker working unchanged (PLAINTEXT, no SASL).
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None
    request_timeout_ms: int = 30000
    api_version_auto_timeout_ms: int = 10000

    def is_sasl(self) -> bool:
        return self.security_protocol.upper() in ("SASL_SSL", "SASL_PLAINTEXT")


@dataclasses.dataclass
class SimulationConfig:
    speed_factor: float
    duration_minutes: float
    random_seed: Optional[int]
    start_sim_hour: float


@dataclasses.dataclass
class ScaleConfig:
    num_zones: int
    num_customers: int
    num_restaurants: int
    num_delivery_partners: int


@dataclasses.dataclass
class DemandConfig:
    base_orders_per_min: float
    peak_windows: list
    off_peak_multiplier: float
    zone_demand_skew: bool


@dataclasses.dataclass
class SupplyConfig:
    base_dp_online_ratio: float
    peak_dp_online_ratio: float
    shortage_probability: float
    shortage_multiplier: float
    dp_speed_kmph_range: list
    traffic_slowdown_probability: float
    traffic_slowdown_factor: float


@dataclasses.dataclass
class RestaurantConfig:
    avg_prep_minutes_range: list
    prep_variance_minutes: float
    accept_sla_seconds: float
    reject_probability: float
    no_response_probability: float
    capacity_orders_range: list
    offline_probability_per_hour: float


@dataclasses.dataclass
class OrderConfig:
    cancel_before_accept_probability: float
    cancel_after_accept_probability: float
    cancel_after_ready_probability: float
    items_per_order_range: list
    avg_item_price_inr: float


@dataclasses.dataclass
class PaymentConfig:
    base_failure_rate: float
    gateway_outage_probability_per_hour: float
    gateway_outage_failure_rate: float
    gateway_outage_duration_minutes_range: list
    max_retries: int
    retry_backoff_seconds_range: list


@dataclasses.dataclass
class DispatchConfig:
    batching_enabled: bool
    batching_max_detour_km: float
    batching_probability_when_eligible: float
    candidate_pool_size: int
    gps_ping_interval_seconds: float


@dataclasses.dataclass
class ChaosConfig:
    duplicate_event_probability: float
    late_event_probability: float
    late_event_delay_seconds_range: list
    out_of_order_reorder_window: int
    gps_dropout_probability: float
    gps_dropout_pings: list
    gps_anomaly_probability: float


@dataclasses.dataclass
class AppConfig:
    output: OutputConfig
    kafka: KafkaConfig
    simulation: SimulationConfig
    scale: ScaleConfig
    demand: DemandConfig
    supply: SupplyConfig
    restaurant: RestaurantConfig
    order: OrderConfig
    payment: PaymentConfig
    dispatch: DispatchConfig
    chaos: ChaosConfig

    @staticmethod
    def load(path: str | Path) -> "AppConfig":
        with open(path, "r") as f:
            raw_text = f.read()
        raw_text = _expand_env_vars(raw_text)
        raw: dict[str, Any] = yaml.safe_load(raw_text)

        return AppConfig(
            output=OutputConfig(**raw["output"]),
            kafka=KafkaConfig(**raw["kafka"]),
            simulation=SimulationConfig(**raw["simulation"]),
            scale=ScaleConfig(**raw["scale"]),
            demand=DemandConfig(
                base_orders_per_min=raw["demand"]["base_orders_per_min"],
                peak_windows=[PeakWindow(**pw) for pw in raw["demand"]["peak_windows"]],
                off_peak_multiplier=raw["demand"]["off_peak_multiplier"],
                zone_demand_skew=raw["demand"]["zone_demand_skew"],
            ),
            supply=SupplyConfig(**raw["supply"]),
            restaurant=RestaurantConfig(**raw["restaurant"]),
            order=OrderConfig(**raw["order"]),
            payment=PaymentConfig(**raw["payment"]),
            dispatch=DispatchConfig(**raw["dispatch"]),
            chaos=ChaosConfig(**raw["chaos"]),
        )
