"""Publish raw bytes directly to a Kafka topic — deliberately bypassing the
platform's own Pydantic envelope validation.

The ingestion-service's ``EventEnvelope`` model would reject a poison event
before it ever reached Kafka, which is correct for the ingestion API but
useless for testing what the *downstream* pipeline (Spark Structured
Streaming, or the processing-service consumer) does when a poison message
somehow lands on a topic anyway (a misbehaving producer, a manual publish,
a schema-version mismatch from an older client, ...). This module publishes
raw bytes with the stdlib ``kafka-python`` client, no envelope validation
in the way.
"""

from __future__ import annotations

from typing import Any


def publish_raw(bootstrap_servers: str, topic: str, key: str, value: str, timeout_seconds: float = 5.0) -> dict[str, Any]:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id="reliability-injector",
        key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        request_timeout_ms=int(timeout_seconds * 1000),
    )
    try:
        future = producer.send(topic, key=key, value=value)
        metadata = future.get(timeout=timeout_seconds)
        return {"topic": metadata.topic, "partition": metadata.partition, "offset": metadata.offset}
    finally:
        producer.close(timeout=timeout_seconds)


def publish_raw_twice(bootstrap_servers: str, topic: str, key: str, value: str) -> list[dict[str, Any]]:
    """Publish the same message twice — for the duplicate-event exercise."""
    return [publish_raw(bootstrap_servers, topic, key, value) for _ in range(2)]
