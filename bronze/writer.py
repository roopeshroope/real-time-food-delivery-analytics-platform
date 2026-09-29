"""
Writes one topic's parsed stream out to two Delta sinks in the same
micro-batch: the topic's own Bronze table, and the shared
`malformed_events` quarantine table.

Idempotency: a single Structured Streaming query can still redeliver the
same micro-batch more than once (e.g. the batch's Bronze write succeeds but
the malformed write throws, and the whole batch retries). Plain `.mode
("append")` would double-insert rows in that case. Delta Lake's
`txnAppId` / `txnVersion` write options solve exactly this: Delta records
`(txnAppId, txnVersion)` in its transaction log and silently no-ops a write
that replays a version it has already committed for that app id --
Databricks' documented pattern for idempotent multi-sink `foreachBatch`
writes. `txnAppId` is fixed per (topic, sink); `txnVersion` is the
micro-batch id Structured Streaming already hands us.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from bronze.config import BronzeConfig, BronzeTopicConfig
from bronze.schemas import SCHEMAS
from bronze.transform import parse_and_enrich
from bronze.kafka_reader import read_kafka_stream

log = logging.getLogger("bronze.writer")


def _idempotent_append(df: DataFrame, path: str, table_fqn: str, txn_app_id: str, batch_id: int) -> None:
    (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", txn_app_id)
        .option("txnVersion", str(batch_id))
        .option("mergeSchema", "true")   # tolerate additive, nullable new columns after a code deploy
        .save(path)
    )
    log.debug("wrote batch=%s rows=%d -> %s (%s)", batch_id, df.count(), table_fqn, path)


def run_topic_stream(spark, cfg: BronzeConfig, topic_key: str) -> StreamingQuery:
    topic_cfg: BronzeTopicConfig = cfg.topics[topic_key]
    schema = SCHEMAS[topic_cfg.schema_ref]

    raw_df = read_kafka_stream(spark, cfg, topic_cfg)

    bronze_table_fqn = cfg.table_fqn(topic_cfg.bronze_table)
    bronze_path = cfg.storage_path(topic_cfg.bronze_table)
    malformed_table_fqn = cfg.table_fqn(cfg.malformed_table)
    malformed_path = cfg.storage_path(cfg.malformed_table)

    good_txn_app_id = f"bronze-{topic_key}"
    malformed_txn_app_id = f"bronze-{topic_key}-malformed"

    def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.persist()
        try:
            good_df, malformed_df = parse_and_enrich(batch_df, schema, topic_key)

            good_count = good_df.count()
            if good_count > 0:
                _idempotent_append(good_df, bronze_path, bronze_table_fqn, good_txn_app_id, batch_id)

            malformed_count = malformed_df.count()
            if malformed_count > 0:
                _idempotent_append(
                    malformed_df, malformed_path, malformed_table_fqn, malformed_txn_app_id, batch_id
                )
                log.warning(
                    "topic=%s batch=%s quarantined %d malformed record(s)",
                    topic_cfg.kafka_topic, batch_id, malformed_count,
                )

            log.info(
                "topic=%s batch=%s good=%d malformed=%d",
                topic_cfg.kafka_topic, batch_id, good_count, malformed_count,
            )
        finally:
            batch_df.unpersist()

    query = (
        raw_df.writeStream.foreachBatch(_process_batch)
        .option("checkpointLocation", cfg.checkpoint_path(topic_cfg.bronze_table))
        .trigger(processingTime=f"{topic_cfg.trigger_seconds} seconds")
        .queryName(f"bronze_{topic_key}")
        .start()
    )
    log.info(
        "Started stream '%s' | topic=%s -> %s | checkpoint=%s",
        query.name, topic_cfg.kafka_topic, bronze_table_fqn, cfg.checkpoint_path(topic_cfg.bronze_table),
    )
    return query
