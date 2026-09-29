# Databricks notebook source
"""
Parameterized Bronze ingestion entrypoint. One process, one topic (or `all`
topics in a single driver for local/dev smoke-testing) -- in production each
topic_key is a separate Databricks Job task (see resources/bronze_job.yml)
running this same script with a different `--topic-key`, so a slow/broken
topic never blocks another, and each has its own checkpoint and can be
individually restarted or scaled.

Usage:
    # Databricks job task parameter, or local CLI:
    python -m bronze.run_bronze_stream --topic-key gps_pings

    # local smoke test against a local Kafka + local Delta path, all 6 topics
    # in one process:
    python -m bronze.run_bronze_stream --topic-key all --local-fallback

Restart / recovery: nothing here is stateful across restarts except the
Structured Streaming checkpoint at `cfg.checkpoint_path(...)` -- killing and
re-running this process (or letting the Databricks Job's automatic retry
restart it) resumes each topic from its last committed Kafka offset with no
data loss and no manual intervention.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

# allow `python -m bronze.run_bronze_stream` from the repo root, and running
# this file directly as a Databricks notebook/task (which doesn't always
# put the repo root on sys.path automatically)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bronze.config import BronzeConfig, resolve_local_fallback  # noqa: E402
from bronze.writer import run_topic_stream  # noqa: E402
from simulator.logging_config import setup_logging  # noqa: E402

log = logging.getLogger("bronze.run")


def get_spark_session():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        # Running as a Databricks job/notebook: the cluster's SparkSession is
        # already Delta- and Unity-Catalog-enabled. Never reconfigure it here.
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

    # Local/dev: build a Delta-enabled local SparkSession (requires
    # `delta-spark` -- see requirements.txt).
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("bronze-local")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--topic-key", required=True,
                    help="One of the keys in config/bronze_config.yaml's topics section, or 'all'")
    p.add_argument("--app-config", default=None, help="Override path to config/config.yaml")
    p.add_argument("--bronze-config", default=None, help="Override path to config/bronze_config.yaml")
    p.add_argument("--local-fallback", action="store_true",
                    help="Redirect storage_root/checkpoint_root to a local ./bronze_lake directory "
                         "instead of abfss:// -- for local dev/CI, never for production")
    p.add_argument("--local-root", default="./bronze_lake")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    load_kwargs = {}
    if args.bronze_config:
        load_kwargs["bronze_config_path"] = args.bronze_config
    if args.app_config:
        load_kwargs["app_config_path"] = args.app_config
    cfg: BronzeConfig = BronzeConfig.load(**load_kwargs)

    if args.local_fallback:
        cfg = resolve_local_fallback(cfg, args.local_root)
        log.warning("--local-fallback set: writing to %s (NOT abfss://, dev/CI only)", cfg.storage_root)

    if args.topic_key not in cfg.topics and args.topic_key != "all":
        raise SystemExit(
            f"--topic-key='{args.topic_key}' not found in bronze_config.yaml. "
            f"Valid keys: {sorted(cfg.topics)} or 'all'."
        )

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    topic_keys = list(cfg.topics) if args.topic_key == "all" else [args.topic_key]
    queries = [run_topic_stream(spark, cfg, tk) for tk in topic_keys]

    log.info("Bronze ingestion running for topics=%s. Ctrl+C to stop.", topic_keys)
    try:
        # awaitAnyTermination + a poll loop (rather than a plain
        # awaitTermination per query) so if one topic's stream dies we log
        # it and keep the others running instead of the whole process
        # blocking silently on a query that already failed.
        while True:
            for q in queries:
                if not q.isActive:
                    exc = q.exception()
                    log.error("Stream '%s' is no longer active. exception=%s", q.name, exc)
            spark.streams.awaitAnyTermination(timeout=30)
            if all(not q.isActive for q in queries):
                break
    except KeyboardInterrupt:
        log.info("Interrupted -- stopping all streams cleanly.")
        for q in queries:
            q.stop()


if __name__ == "__main__":
    main()
