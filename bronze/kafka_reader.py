"""
Builds the raw Kafka `readStream` DataFrame for one topic.

Connection options mirror `simulator/kafka_producer.py`'s `_init_kafka()`
exactly (same security_protocol / sasl_mechanism / API key+secret shape) so
the producer and this consumer are always pointed at the same cluster with
the same credentials, sourced the same way.

Databricks secrets: rather than special-casing `dbutils.secrets.get(...)`
here, set the KAFKA_API_KEY / KAFKA_API_SECRET job or cluster environment
variables to Databricks' native `{{secrets/<scope>/<key>}}` reference syntax
-- Databricks resolves those into real env var values at cluster start, so
this module (and simulator.config.AppConfig, which reads os.environ exactly
like the producer does) needs zero Databricks-specific code. See
resources/bronze_job.yml for where that's wired.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from bronze.config import BronzeConfig, BronzeTopicConfig

log = logging.getLogger("bronze.kafka_reader")


def _jaas_config(username: str, password: str) -> str:
    # Escape the same way the JAAS config format expects; Confluent Cloud
    # API keys/secrets don't contain quotes or semicolons in practice, but
    # we don't trust that blindly.
    username = username.replace('"', '\\"')
    password = password.replace('"', '\\"')
    return (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{username}" password="{password}";'
    )


def read_kafka_stream(spark: SparkSession, cfg: BronzeConfig, topic_cfg: BronzeTopicConfig) -> DataFrame:
    kcfg = cfg.app_config.kafka

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kcfg.bootstrap_servers)
        .option("subscribe", topic_cfg.kafka_topic)
        .option("startingOffsets", topic_cfg.starting_offsets)
        .option("maxOffsetsPerTrigger", str(topic_cfg.max_offsets_per_trigger))
        # A topic that's been compacted/retention-expired past our checkpoint's
        # offsets should not silently kill the stream -- log and skip instead.
        # Bronze favors availability; anything actually lost is visible via
        # the consumer-lag / "earliest available offset" monitoring in
        # scripts/validate_bronze.py rather than a crashed job.
        .option("failOnDataLoss", "false")
    )

    if kcfg.is_sasl():
        missing = [
            name for name, val in [
                ("sasl_mechanism", kcfg.sasl_mechanism),
                ("sasl_plain_username (KAFKA_API_KEY)", kcfg.sasl_plain_username),
                ("sasl_plain_password (KAFKA_API_SECRET)", kcfg.sasl_plain_password),
            ] if not val
        ]
        if missing:
            raise RuntimeError(
                f"kafka.security_protocol={kcfg.security_protocol} but missing: "
                f"{', '.join(missing)}. Set the corresponding environment variables "
                f"(or Databricks {{{{secrets/scope/key}}}} cluster env vars)."
            )
        reader = (
            reader.option("kafka.security.protocol", kcfg.security_protocol)
            .option("kafka.sasl.mechanism", kcfg.sasl_mechanism)
            .option(
                "kafka.sasl.jaas.config",
                _jaas_config(kcfg.sasl_plain_username, kcfg.sasl_plain_password),
            )
        )
    else:
        reader = reader.option("kafka.security.protocol", kcfg.security_protocol)

    log.info(
        "Reading topic=%s from %s (security_protocol=%s, startingOffsets=%s)",
        topic_cfg.kafka_topic, kcfg.bootstrap_servers, kcfg.security_protocol,
        topic_cfg.starting_offsets,
    )
    return reader.load()
