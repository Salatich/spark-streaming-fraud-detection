"""Kafka client for reading real-time alerts and DLQ quarantine records for the UI."""

from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger("UI_KafkaClient")


def get_consumer(topic: str, group_id: str | None = None) -> Consumer:
    """Create a temporary Kafka consumer for real-time dashboard inspection."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    gid = group_id or f"streamlit-ui-{uuid.uuid4().hex[:8]}"

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": gid,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    return consumer


def consume_recent_messages(topic: str, max_messages: int = 50) -> list[dict[str, Any]]:
    """Poll up to `max_messages` from the beginning of a given topic."""
    messages: list[dict[str, Any]] = []
    consumer = None
    try:
        consumer = get_consumer(topic)
        empty_polls = 0
        while len(messages) < max_messages and empty_polls < 3:
            msg = consumer.poll(timeout=0.6)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_polls += 1
                    continue
                logger.warning(f"Kafka error on topic {topic}: {msg.error()}")
                break

            empty_polls = 0
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                messages.append(payload)
            except Exception as e:
                messages.append(
                    {
                        "raw": msg.value().decode("utf-8", errors="replace"),
                        "error": str(e),
                    }
                )
    except Exception as e:
        logger.warning(f"Failed to consume from Kafka topic '{topic}': {e}")
    finally:
        if consumer:
            consumer.close()

    return list(reversed(messages))
