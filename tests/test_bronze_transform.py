"""
Unit tests for bronze/transform.py using a local Spark + Delta session --
no Kafka, no Databricks required. Run with:

    pytest tests/test_bronze_transform.py -v

These construct a DataFrame shaped exactly like what `spark.readStream
.format("kafka")` produces (key, value, topic, partition, offset,
timestamp, timestampType columns) so parse_and_enrich() is tested against
its real input contract, not a simplified stand-in.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

SAMPLES = Path(__file__).resolve().parent / "sample_events"


@pytest.fixture(scope="module")
def spark():
    # transform.py uses only core Spark SQL functions (from_json, lit,
    # current_timestamp, ...) -- no Delta APIs -- so these tests don't need
    # Delta configured at all. writer.py's actual Delta write path is a
    # thin, standard use of the documented Delta writer API and is exercised
    # in a real environment via `run_bronze_stream.py --local-fallback`
    # (requires network access to Maven Central for the Delta JAR, which a
    # normal dev machine has).
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("bronze-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _make_kafka_like_df(spark, values: list[str], topic: str = "order-lifecycle-events"):
    from pyspark.sql import Row
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BinaryType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
    )

    kafka_source_schema = StructType([
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("timestampType", IntegerType(), True),
    ])

    rows = [
        Row(
            key=f"key-{i}".encode("utf-8"),
            value=(v.encode("utf-8") if v is not None else None),
            topic=topic,
            partition=0,
            offset=i,
            timestamp=None,
            timestampType=0,
        )
        for i, v in enumerate(values)
    ]
    df = spark.createDataFrame(rows, schema=kafka_source_schema)
    # real Kafka source rows always have a non-null broker timestamp; fill
    # it here rather than leaving the all-null placeholder above.
    return df.withColumn("timestamp", F.current_timestamp())


def test_valid_event_lands_in_good_df(spark):
    from bronze.schemas import ORDER_LIFECYCLE_SCHEMA
    from bronze.transform import parse_and_enrich

    valid_json = SAMPLES.joinpath("order_lifecycle_valid.json").read_text().strip()
    df = _make_kafka_like_df(spark, [valid_json])

    good_df, malformed_df = parse_and_enrich(df, ORDER_LIFECYCLE_SCHEMA, "order_lifecycle")

    assert good_df.count() == 1
    assert malformed_df.count() == 0

    row = good_df.first()
    assert row["order_id"] == "ORD-000000000001"
    assert row["state"] == "PLACED"
    assert row["item_count"] == 3
    assert row["raw_value"] == valid_json
    assert row["kafka_topic"] == "order-lifecycle-events"
    assert row["kafka_offset"] == 0
    assert row["ingestion_timestamp"] is not None
    assert row["ingest_date"] is not None


def test_unparsable_json_is_quarantined(spark):
    from bronze.schemas import ORDER_LIFECYCLE_SCHEMA
    from bronze.transform import parse_and_enrich

    bad = SAMPLES.joinpath("unparsable.txt").read_text().strip()
    df = _make_kafka_like_df(spark, [bad])

    good_df, malformed_df = parse_and_enrich(df, ORDER_LIFECYCLE_SCHEMA, "order_lifecycle")

    assert good_df.count() == 0
    assert malformed_df.count() == 1
    row = malformed_df.first()
    assert row["error_reason"] == "unparsable_json"
    assert row["raw_value"] == bad


def test_missing_required_field_is_quarantined(spark):
    from bronze.schemas import ORDER_LIFECYCLE_SCHEMA
    from bronze.transform import parse_and_enrich

    missing = SAMPLES.joinpath("order_lifecycle_missing_event_id.json").read_text().strip()
    df = _make_kafka_like_df(spark, [missing])

    good_df, malformed_df = parse_and_enrich(df, ORDER_LIFECYCLE_SCHEMA, "order_lifecycle")

    assert good_df.count() == 0
    assert malformed_df.count() == 1
    assert malformed_df.first()["error_reason"] == "missing_required_fields"


def test_mixed_batch_splits_correctly(spark):
    from bronze.schemas import ORDER_LIFECYCLE_SCHEMA
    from bronze.transform import parse_and_enrich

    valid_json = SAMPLES.joinpath("order_lifecycle_valid.json").read_text().strip()
    bad = SAMPLES.joinpath("unparsable.txt").read_text().strip()
    missing = SAMPLES.joinpath("order_lifecycle_missing_event_id.json").read_text().strip()

    df = _make_kafka_like_df(spark, [valid_json, bad, missing, valid_json])

    good_df, malformed_df = parse_and_enrich(df, ORDER_LIFECYCLE_SCHEMA, "order_lifecycle")

    assert good_df.count() == 2
    assert malformed_df.count() == 2
    reasons = {r["error_reason"] for r in malformed_df.collect()}
    assert reasons == {"unparsable_json", "missing_required_fields"}


def test_all_six_schemas_are_distinct_and_have_base_fields():
    from bronze.schemas import SCHEMAS

    assert set(SCHEMAS.keys()) == {
        "order_lifecycle", "dp_status", "gps_pings",
        "restaurant_status", "payment", "dispatch_decision",
    }
    for key, schema in SCHEMAS.items():
        names = {f.name for f in schema.fields}
        assert {"event_time", "schema_version", "event_id"}.issubset(names), key
