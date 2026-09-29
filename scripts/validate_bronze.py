#!/usr/bin/env python3
"""
"Is Kafka -> Bronze actually working?" -- run this after starting the
simulator (or against Confluent Cloud) and the Bronze streams, and it
answers that in one shot: row counts per table, freshness (how stale is the
newest row), and the malformed-record rate.

Reads by Delta PATH (not by Unity Catalog table name), so this works
identically against a local --local-fallback run and a real Databricks/UC
deployment without needing catalog access.

Usage:
    python -m scripts.validate_bronze                        # Databricks / real deployment
    python -m scripts.validate_bronze --local-fallback        # local dev run
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bronze.config import BronzeConfig, resolve_local_fallback  # noqa: E402
from simulator.logging_config import setup_logging  # noqa: E402

log = logging.getLogger("validate_bronze")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-fallback", action="store_true")
    parser.add_argument("--local-root", default="./bronze_lake")
    args = parser.parse_args()
    setup_logging("INFO")

    cfg = BronzeConfig.load()
    if args.local_fallback:
        cfg = resolve_local_fallback(cfg, args.local_root)

    from bronze.run_bronze_stream import get_spark_session
    from pyspark.sql import functions as F

    spark = get_spark_session()

    print(f"\n{'topic_key':<20}{'table':<28}{'rows':>10}{'newest_event':>26}{'freshness':>14}")
    print("-" * 100)
    any_rows = False
    for topic_key, topic_cfg in cfg.topics.items():
        path = cfg.storage_path(topic_cfg.bronze_table)
        try:
            df = spark.read.format("delta").load(path)
        except Exception as e:
            print(f"{topic_key:<20}{topic_cfg.bronze_table:<28}{'<no data yet>':>10}   ({e.__class__.__name__})")
            continue

        row_count = df.count()
        if row_count == 0:
            print(f"{topic_key:<20}{topic_cfg.bronze_table:<28}{0:>10}")
            continue

        any_rows = True
        stats = df.select(
            F.max("ingestion_timestamp").alias("newest_ingest"),
            F.max("event_time").alias("newest_event"),
        ).first()
        freshness = spark.sql(
            f"SELECT CAST(current_timestamp() AS LONG) - CAST('{stats['newest_ingest']}' AS LONG) AS s"
        ).first()["s"]
        print(f"{topic_key:<20}{topic_cfg.bronze_table:<28}{row_count:>10}{str(stats['newest_event']):>26}{f'{freshness}s ago':>14}")

    malformed_path = cfg.storage_path(cfg.malformed_table)
    print("\n--- malformed_events (quarantine) ---")
    try:
        mdf = spark.read.format("delta").load(malformed_path)
        mcount = mdf.count()
        print(f"total malformed rows: {mcount}")
        if mcount > 0:
            print("by topic / reason:")
            mdf.groupBy("source_topic_key", "error_reason").count().orderBy(F.desc("count")).show(truncate=False)
    except Exception as e:
        print(f"<no malformed table / no data yet> ({e.__class__.__name__})")

    if not any_rows:
        log.warning(
            "No rows in ANY Bronze table yet. Checklist: (1) is the simulator actually "
            "producing (output.mode: kafka)? (2) are the Bronze streams running "
            "(run_bronze_stream.py)? (3) does bootstrap_servers/topic name match on both "
            "sides -- both read from config/config.yaml, so check env vars are set the "
            "same way for both processes."
        )


if __name__ == "__main__":
    main()
