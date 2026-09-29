#!/usr/bin/env python3
"""
Pre-creates the 6 simulator topics on a Kafka cluster that doesn't
auto-create topics on publish (Confluent Cloud has auto-create OFF by
default for any cluster above the free "Basic" tier's default settings, and
many teams disable it deliberately even there).

Safe to run against a local docker-compose broker too -- it just no-ops on
topics that already exist.

Usage:
    python scripts/create_topics.py --config config/config.yaml
    python scripts/create_topics.py --config config/config.yaml --dry-run

Partition counts are intentionally uneven: gps-pings is by far the highest
-volume topic (see README's volume table), so it gets more partitions to
parallelize consumers against it.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from simulator.config import AppConfig  # noqa: E402
from simulator.logging_config import setup_logging  # noqa: E402

log = logging.getLogger("create_topics")

# topic_key (from config.kafka.topics) -> (partitions, replication_factor)
# replication_factor is ignored by Confluent Cloud (it manages this itself
# and will error if you pass anything other than what the cluster requires
# for its tier) -- kept configurable for a self-managed / docker-compose broker.
TOPIC_SPECS: dict[str, tuple[int, int]] = {
    "order_lifecycle": (6, 3),
    "dp_status": (6, 3),
    "gps_pings": (12, 3),
    "restaurant_status": (3, 3),
    "payment": (6, 3),
    "dispatch_decision": (6, 3),
}


def build_admin_client(cfg: AppConfig):
    from kafka.admin import KafkaAdminClient

    kcfg = cfg.kafka
    kwargs: dict = dict(
        bootstrap_servers=kcfg.bootstrap_servers,
        client_id="fd-sim-topic-bootstrap",
        security_protocol=kcfg.security_protocol,
        request_timeout_ms=kcfg.request_timeout_ms,
        api_version_auto_timeout_ms=kcfg.api_version_auto_timeout_ms,
    )
    if kcfg.is_sasl():
        kwargs.update(
            sasl_mechanism=kcfg.sasl_mechanism,
            sasl_plain_username=kcfg.sasl_plain_username,
            sasl_plain_password=kcfg.sasl_plain_password,
        )
    return KafkaAdminClient(**kwargs)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be created, don't call Kafka")
    parser.add_argument("--replication-factor", type=int, default=None,
                         help="Override every topic's replication factor "
                              "(Confluent Cloud: usually omit this / leave the default)")
    args = parser.parse_args()
    setup_logging("INFO")

    cfg = AppConfig.load(args.config)
    is_confluent = "confluent.cloud" in cfg.kafka.bootstrap_servers

    plan = []
    for topic_key, physical_name in cfg.kafka.topics.items():
        partitions, default_rf = TOPIC_SPECS.get(topic_key, (6, 3))
        rf = args.replication_factor if args.replication_factor is not None else default_rf
        plan.append((physical_name, partitions, rf))

    log.info("Target cluster: %s (%s)", cfg.kafka.bootstrap_servers,
              "Confluent Cloud" if is_confluent else "self-managed")
    for name, partitions, rf in plan:
        log.info("  %-30s partitions=%-3d replication_factor=%d", name, partitions, rf)

    if args.dry_run:
        log.info("--dry-run set, not creating anything.")
        return

    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = build_admin_client(cfg)
    try:
        new_topics = [
            NewTopic(name=name, num_partitions=partitions, replication_factor=rf)
            for name, partitions, rf in plan
        ]
        try:
            admin.create_topics(new_topics=new_topics, validate_only=False)
            log.info("All topics created.")
        except TopicAlreadyExistsError:
            # kafka-python raises this for the whole batch even if only one
            # topic already existed -- fall back to creating one at a time
            # so partial progress isn't blocked by topics that already exist.
            log.info("Some topics already exist; creating the remainder individually.")
            for name, partitions, rf in plan:
                try:
                    admin.create_topics(
                        new_topics=[NewTopic(name=name, num_partitions=partitions, replication_factor=rf)],
                        validate_only=False,
                    )
                    log.info("  created %s", name)
                except TopicAlreadyExistsError:
                    log.info("  %s already exists, skipping", name)
    finally:
        admin.close()


if __name__ == "__main__":
    main()
