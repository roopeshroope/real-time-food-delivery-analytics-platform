#!/usr/bin/env python3
"""
Applies ddl/create_bronze_tables.sql, substituting ${catalog} / ${bronze_schema}
/ ${storage_root} from the SAME BronzeConfig the ingestion pipeline reads --
so the tables Unity Catalog knows about and the paths run_bronze_stream.py
actually writes to can never drift apart.

Usage:
    # against a Databricks cluster/SQL warehouse (run from a notebook, or
    # via `databricks-connect` / `spark-submit` with cluster access):
    python -m scripts.apply_ddl

    # sanity-check the rendered SQL (placeholders substituted, LOCATION
    # paths correct) without touching any cluster -- safe to run anywhere,
    # including this repo's local dev environment:
    python -m scripts.apply_ddl --dry-run --local-fallback

Note: `CREATE CATALOG` is a Unity Catalog construct with no OSS-Spark
equivalent, so non-dry-run execution only makes sense against a real
Databricks workspace -- `--local-fallback` is meant to be paired with
`--dry-run` locally, not run standalone against a local SparkSession.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bronze.config import BronzeConfig, resolve_local_fallback  # noqa: E402
from simulator.logging_config import setup_logging  # noqa: E402

log = logging.getLogger("apply_ddl")

DDL_PATH = Path(__file__).resolve().parent.parent / "ddl" / "create_bronze_tables.sql"


def render_statements(cfg: BronzeConfig) -> list[str]:
    text = DDL_PATH.read_text()
    text = (
        text.replace("${catalog}", cfg.catalog)
        .replace("${bronze_schema}", cfg.bronze_schema)
        .replace("${storage_root}", cfg.storage_root.rstrip("/"))
    )
    # naive but sufficient statement split -- the DDL file has no semicolons
    # inside string literals or comments that would confuse this
    statements = [s.strip() for s in text.split(";")]
    return [s for s in statements if s and not s.startswith("--")]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-fallback", action="store_true")
    parser.add_argument("--local-root", default="./bronze_lake")
    parser.add_argument("--dry-run", action="store_true", help="Print statements, don't execute")
    args = parser.parse_args()
    setup_logging("INFO")

    cfg = BronzeConfig.load()
    if args.local_fallback:
        cfg = resolve_local_fallback(cfg, args.local_root)

    statements = render_statements(cfg)

    if args.dry_run:
        for s in statements:
            log.info("-- statement --\n%s", s)
        return

    from bronze.run_bronze_stream import get_spark_session
    spark = get_spark_session()

    for stmt in statements:
        log.info("Executing: %s", stmt.splitlines()[0][:100])
        spark.sql(stmt)

    log.info("Applied %d DDL statements against catalog=%s schema=%s",
              len(statements), cfg.catalog, cfg.bronze_schema)


if __name__ == "__main__":
    main()
