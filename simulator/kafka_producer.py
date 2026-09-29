"""
Thin producer abstraction over kafka-python with two testing fallbacks:

  - mode="kafka"   : real publish to the configured Kafka cluster
  - mode="console" : pretty-print events to stdout (no broker needed)
  - mode="file"     : append newline-delimited JSON per topic under file_dir

This lets you validate the simulator's event logic end-to-end before you've
even stood up Kafka locally (see README "Dry run without Kafka").

Chaos (duplicate / late / out-of-order emission) is applied HERE, at the
publish boundary, so every generator gets it for free without each one
re-implementing the same logic.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import threading
import time
from collections import deque
from pathlib import Path
from typing import Optional

from simulator.config import AppConfig
from simulator.schemas import BaseEvent

log = logging.getLogger("producer")


class EventPublisher:
    def __init__(self, cfg: AppConfig, rng: random.Random):
        self.cfg = cfg
        self.rng = rng
        self.mode = cfg.output.mode
        self._kafka_producer = None
        self._file_handles: dict[str, "TextIOWrapper"] = {}
        self._reorder_buffers: dict[str, deque] = {}
        self._lock = threading.Lock()
        self._sent_count = 0
        self._error_count = 0

        if self.mode == "kafka":
            self._init_kafka()
        elif self.mode == "file":
            outdir = Path(cfg.output.file_dir)
            outdir.mkdir(parents=True, exist_ok=True)
            self._file_dir = outdir

    def _init_kafka(self) -> None:
        try:
            from kafka import KafkaProducer
        except ImportError as e:
            raise RuntimeError(
                "kafka-python is not installed. `pip install kafka-python` "
                "or set output.mode to 'console'/'file' for a dry run."
            ) from e

        kcfg = self.cfg.kafka
        producer_kwargs: dict = dict(
            bootstrap_servers=kcfg.bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=kcfg.acks,
            linger_ms=kcfg.linger_ms,
            retries=kcfg.retries,
            compression_type=kcfg.compression_type,
            max_in_flight_requests_per_connection=5,
            security_protocol=kcfg.security_protocol,
            request_timeout_ms=kcfg.request_timeout_ms,
            api_version_auto_timeout_ms=kcfg.api_version_auto_timeout_ms,
        )

        if kcfg.is_sasl():
            # Confluent Cloud (and any SASL_SSL-secured cluster): API key/secret
            # are the SASL username/password under the PLAIN mechanism.
            # kafka-python opens a default SSL context automatically for the
            # SASL_SSL protocol -- no ssl_cafile needed for Confluent Cloud's
            # publicly-trusted certs.
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
                    f"{', '.join(missing)}. Set the corresponding environment "
                    f"variables (see .env.example) before running against Confluent Cloud."
                )
            producer_kwargs.update(
                sasl_mechanism=kcfg.sasl_mechanism,
                sasl_plain_username=kcfg.sasl_plain_username,
                sasl_plain_password=kcfg.sasl_plain_password,
            )

        self._kafka_producer = KafkaProducer(**producer_kwargs)
        log.info(
            "Kafka producer connected to %s (security_protocol=%s)",
            kcfg.bootstrap_servers, kcfg.security_protocol,
        )

    # -- public API -----------------------------------------------------
    async def publish(self, topic_key: str, key: str, event: BaseEvent) -> None:
        """
        topic_key is the logical name from config.kafka.topics (e.g. "gps_pings"),
        NOT the raw Kafka topic string -- this indirection lets you rename
        physical topics without touching generator code.
        """
        payload = event.to_dict()
        topic = self.cfg.kafka.topics[topic_key]

        # --- chaos: duplicate emission ---
        emit_count = 1
        if self.rng.random() < self.cfg.chaos.duplicate_event_probability:
            emit_count = 2
            log.debug("chaos: duplicating event_id=%s on %s", payload["event_id"], topic)

        # --- chaos: late / out-of-order emission ---
        delay = 0.0
        if self.rng.random() < self.cfg.chaos.late_event_probability:
            lo, hi = self.cfg.chaos.late_event_delay_seconds_range
            delay = self.rng.uniform(lo, hi)
            log.debug("chaos: delaying event_id=%s by %.1fs on %s", payload["event_id"], delay, topic)

        for _ in range(emit_count):
            if delay > 0:
                asyncio.create_task(self._delayed_send(topic, key, payload, delay))
            else:
                self._send(topic, key, payload)

    async def _delayed_send(self, topic: str, key: str, payload: dict, delay: float) -> None:
        await asyncio.sleep(delay)
        self._send(topic, key, payload)

    def _send(self, topic: str, key: str, payload: dict) -> None:
        try:
            if self.mode == "kafka":
                future = self._kafka_producer.send(topic, key=key, value=payload)
                future.add_errback(self._on_kafka_error, topic=topic, event_id=payload.get("event_id"))
            elif self.mode == "console":
                print(f"[{topic}] key={key} {json.dumps(payload)}")
            elif self.mode == "file":
                self._write_file(topic, payload)
            else:
                raise ValueError(f"Unknown output.mode: {self.mode}")

            with self._lock:
                self._sent_count += 1
        except Exception:
            with self._lock:
                self._error_count += 1
            log.exception("Failed to publish event to %s", topic)

    def _write_file(self, topic: str, payload: dict) -> None:
        with self._lock:
            fh = self._file_handles.get(topic)
            if fh is None:
                fh = open(self._file_dir / f"{topic}.jsonl", "a", buffering=1)
                self._file_handles[topic] = fh
            fh.write(json.dumps(payload) + "\n")

    def _on_kafka_error(self, exc, topic: str, event_id: str) -> None:
        with self._lock:
            self._error_count += 1
        log.error("Kafka send failed topic=%s event_id=%s err=%s", topic, event_id, exc)

    def stats(self) -> dict:
        with self._lock:
            return {"sent": self._sent_count, "errors": self._error_count}

    def close(self) -> None:
        if self.mode == "kafka" and self._kafka_producer is not None:
            self._kafka_producer.flush(timeout=10)
            self._kafka_producer.close(timeout=10)
        for fh in self._file_handles.values():
            fh.close()
        log.info("Publisher closed. stats=%s", self.stats())
