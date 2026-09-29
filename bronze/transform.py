"""
Transform logic shared by every topic: this is deliberately the ONLY place
that decides how a raw Kafka record becomes a Bronze row, so every topic's
Bronze table has the exact same metadata-column shape and the exact same
malformed-record handling -- no per-topic special cases to keep in sync.

No business logic lives here. No joins, no deduplication, no state-machine
validation of `state`/`status` values against simulator/state_machines.py --
that's Silver's job. This module's only opinions are: "is this valid JSON
matching the declared shape or not", and "what did we receive it on/when".
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Kafka's own per-record metadata columns, present on every readStream from
# the "kafka" source format -- documented here so it's obvious which columns
# come from Kafka itself vs. which Bronze adds.
_KAFKA_SOURCE_COLUMNS = ["key", "value", "topic", "partition", "offset", "timestamp", "timestampType"]

_CORRUPT_RECORD_COL = "_corrupt_record"


def _schema_with_corrupt_record(schema: StructType) -> StructType:
    """
    from_json only distinguishes "not valid JSON syntax at all" from "valid
    JSON that's simply missing our required fields" if you give it a
    dedicated column to park the original string in for the former case
    (PERMISSIVE mode's documented behavior) -- so every topic's schema gets
    this field added at parse time only; it never appears in the schemas
    exported from bronze/schemas.py or in any Bronze/quarantine table.
    """
    return StructType(schema.fields + [StructField(_CORRUPT_RECORD_COL, StringType(), True)])


def parse_and_enrich(raw_df: DataFrame, schema: StructType, topic_key: str) -> tuple[DataFrame, DataFrame]:
    """
    Returns (good_df, malformed_df).

    good_df       : one row per event, typed columns flattened from the
                     parsed payload + `raw_value` (untouched original bytes,
                     as a string) + Kafka/ingestion metadata columns.
    malformed_df   : rows whose value either isn't valid JSON at all, or
                     parses but is missing a field the contract requires
                     (event_id / event_time) -- these never silently vanish.
    """
    with_meta = (
        raw_df.withColumn("raw_key", F.col("key").cast("string"))
        .withColumn("raw_value", F.col("value").cast("string"))
        .withColumn("kafka_topic", F.col("topic"))
        .withColumn("kafka_partition", F.col("partition"))
        .withColumn("kafka_offset", F.col("offset"))
        .withColumn("kafka_timestamp", F.col("timestamp"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.col("ingestion_timestamp")))
        .withColumn("source_topic_key", F.lit(topic_key))
    )

    parse_schema = _schema_with_corrupt_record(schema)
    parsed = with_meta.withColumn(
        "_event",
        F.from_json(
            F.col("raw_value"), parse_schema,
            options={"mode": "PERMISSIVE", "columnNameOfCorruptRecord": _CORRUPT_RECORD_COL},
        ),
    )

    # PERMISSIVE mode + columnNameOfCorruptRecord: invalid JSON syntax ->
    # _corrupt_record is populated with the original string and every real
    # field is null. Valid JSON that's simply missing a required key ->
    # _corrupt_record stays null but event_id/event_time are null.
    is_unparsable = F.col(f"_event.{_CORRUPT_RECORD_COL}").isNotNull()
    is_missing_required = ~is_unparsable & (
        F.col("_event.event_id").isNull() | F.col("_event.event_time").isNull()
    )
    malformed_mask: Column = is_unparsable | is_missing_required

    malformed_df = (
        parsed.filter(malformed_mask)
        .withColumn(
            "error_reason",
            F.when(is_unparsable, F.lit("unparsable_json")).otherwise(F.lit("missing_required_fields")),
        )
        .select(
            "source_topic_key", "kafka_topic", "kafka_partition", "kafka_offset",
            "kafka_timestamp", "ingestion_timestamp", "ingest_date",
            "raw_key", "raw_value", "error_reason",
        )
    )

    event_field_names = [f.name for f in schema.fields]  # excludes _corrupt_record
    good_df = (
        parsed.filter(~malformed_mask)
        .select(
            *[F.col(f"_event.{name}").alias(name) for name in event_field_names],
            "raw_value",
            "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
            "ingestion_timestamp", "ingest_date",
        )
    )

    return good_df, malformed_df
