"""
Bronze configuration loader.

Deliberately thin: Kafka CONNECTION settings (bootstrap servers, security
protocol, SASL credentials, and the topic_key -> physical Kafka topic name
mapping) are loaded from THIS SAME REPO's `config/config.yaml` via
`simulator.config.AppConfig` -- the exact object the event simulator itself
uses. Bronze never re-declares a topic name or a credential; it only adds
what's specific to ingestion (table names, triggers, storage paths).

This means: if you rename a Kafka topic or rotate Confluent Cloud
credentials, you change it in ONE place (config/config.yaml /
environment variables) and both the producer and the Bronze consumer pick
it up.
"""
from __future__ import annotations

import dataclasses
import os
from pathlib import Path
from typing import Optional

import yaml

# reuse the simulator's own env-var expansion + AppConfig (single source of
# truth for topic names / Kafka connection details)
from simulator.config import AppConfig, _expand_env_vars

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_APP_CONFIG_PATH = REPO_ROOT / "config" / "config.yaml"
DEFAULT_BRONZE_CONFIG_PATH = REPO_ROOT / "config" / "bronze_config.yaml"


@dataclasses.dataclass
class BronzeTopicConfig:
    topic_key: str                 # e.g. "gps_pings" -- shared with AppConfig.kafka.topics
    kafka_topic: str                # physical Kafka topic name, sourced from AppConfig
    bronze_table: str               # e.g. "gps_pings" -> <catalog>.<schema>.gps_pings
    schema_ref: str                 # key into bronze/schemas.py SCHEMAS
    trigger_seconds: int
    starting_offsets: str
    max_offsets_per_trigger: int


@dataclasses.dataclass
class BronzeConfig:
    app_config: AppConfig                    # simulator's own config (Kafka connection, topics)
    catalog: str
    bronze_schema: str
    storage_root: str
    checkpoint_root: str
    malformed_table: str
    topics: dict[str, BronzeTopicConfig]

    def table_fqn(self, table_name: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{table_name}"

    def storage_path(self, table_name: str) -> str:
        return f"{self.storage_root.rstrip('/')}/{table_name}"

    def checkpoint_path(self, table_name: str) -> str:
        return f"{self.checkpoint_root.rstrip('/')}/{table_name}"

    @staticmethod
    def load(
        bronze_config_path: str | Path = DEFAULT_BRONZE_CONFIG_PATH,
        app_config_path: str | Path = DEFAULT_APP_CONFIG_PATH,
    ) -> "BronzeConfig":
        app_config = AppConfig.load(app_config_path)

        with open(bronze_config_path, "r") as f:
            raw_text = _expand_env_vars(f.read())
        raw: dict = yaml.safe_load(raw_text)

        topics: dict[str, BronzeTopicConfig] = {}
        for topic_key, spec in raw["topics"].items():
            if topic_key not in app_config.kafka.topics:
                raise ValueError(
                    f"bronze_config.yaml references topic_key='{topic_key}' which is not "
                    f"present in config/config.yaml's kafka.topics -- Bronze must map onto "
                    f"a topic the simulator actually publishes, not a new one."
                )
            topics[topic_key] = BronzeTopicConfig(
                topic_key=topic_key,
                kafka_topic=app_config.kafka.topics[topic_key],
                bronze_table=spec["bronze_table"],
                schema_ref=spec["schema_ref"],
                trigger_seconds=spec["trigger_seconds"],
                starting_offsets=spec["starting_offsets"],
                max_offsets_per_trigger=spec["max_offsets_per_trigger"],
            )

        return BronzeConfig(
            app_config=app_config,
            catalog=raw["catalog"],
            bronze_schema=raw["bronze_schema"],
            storage_root=raw["storage_root"],
            checkpoint_root=raw["checkpoint_root"],
            malformed_table=raw["malformed_table"],
            topics=topics,
        )


def resolve_local_fallback(cfg: BronzeConfig, local_root: str = "./bronze_lake") -> BronzeConfig:
    """
    Dev/local convenience: if storage_root/checkpoint_root are unreachable
    abfss:// paths (no Unity Catalog / ADLS credentials configured in this
    environment), redirect both to a local directory so
    run_bronze_stream.py --local-fallback can be smoke-tested without Azure.
    Never used implicitly -- callers opt in explicitly (see main()'s
    --local-fallback flag) so production runs can never silently write to
    the wrong place.
    """
    local = Path(local_root).resolve()
    return dataclasses.replace(
        cfg,
        storage_root=f"file://{local / 'bronze'}",
        checkpoint_root=f"file://{local / '_checkpoints' / 'bronze'}",
    )
