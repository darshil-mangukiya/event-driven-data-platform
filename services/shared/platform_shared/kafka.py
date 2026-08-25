from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from platform_shared.schemas import (
    EVENT_DOMAIN,
    EventEnvelope,
    EventType,
    TopicNames,
    envelope_to_json,
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TopicDefinition:
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int
    description: str


TOPIC_DEFINITIONS: dict[str, TopicDefinition] = {
    TopicNames.ORDER_EVENTS: TopicDefinition(
        TopicNames.ORDER_EVENTS,
        partitions=12,
        replication_factor=1,
        retention_ms=604_800_000,
        description="Order lifecycle events partitioned by tenant_id + order_id where present.",
    ),
    TopicNames.PAYMENT_EVENTS: TopicDefinition(
        TopicNames.PAYMENT_EVENTS,
        partitions=12,
        replication_factor=1,
        retention_ms=604_800_000,
        description="Payment authorization, capture, failure, refund, and risk signals.",
    ),
    TopicNames.USER_EVENTS: TopicDefinition(
        TopicNames.USER_EVENTS,
        partitions=12,
        replication_factor=1,
        retention_ms=259_200_000,
        description="Signup, activity, and churn-related behavior events.",
    ),
    TopicNames.PRODUCT_EVENTS: TopicDefinition(
        TopicNames.PRODUCT_EVENTS,
        partitions=6,
        replication_factor=1,
        retention_ms=604_800_000,
        description="Catalog state and inventory change events.",
    ),
    TopicNames.SYSTEM_EVENTS: TopicDefinition(
        TopicNames.SYSTEM_EVENTS,
        partitions=6,
        replication_factor=1,
        retention_ms=259_200_000,
        description="Service health, pipeline status, and platform system events.",
    ),
    TopicNames.RETRY_EVENTS: TopicDefinition(
        TopicNames.RETRY_EVENTS,
        partitions=6,
        replication_factor=1,
        retention_ms=86_400_000,
        description="Transient failures scheduled for retry with attempt metadata.",
    ),
    TopicNames.DLQ_EVENTS: TopicDefinition(
        TopicNames.DLQ_EVENTS,
        partitions=6,
        replication_factor=1,
        retention_ms=1_209_600_000,
        description="Poison messages and validation failures retained for replay or audit.",
    ),
}


class TopicRouter:
    def route(self, event_type: EventType | str) -> str:
        domain = EVENT_DOMAIN[EventType(event_type)]
        return {
            "orders": TopicNames.ORDER_EVENTS,
            "payments": TopicNames.PAYMENT_EVENTS,
            "users": TopicNames.USER_EVENTS,
            "products": TopicNames.PRODUCT_EVENTS,
            "system": TopicNames.SYSTEM_EVENTS,
        }[domain]

    def partition_key(self, envelope: EventEnvelope) -> bytes:
        domain = EVENT_DOMAIN[EventType(envelope.event_type)]
        business_id = {
            "orders": envelope.payload.get("order_id"),
            "payments": envelope.payload.get("payment_id"),
            "users": envelope.payload.get("user_id"),
            "products": envelope.payload.get("product_id"),
            "system": envelope.payload.get("service_name"),
        }.get(domain) or envelope.event_id
        return f"{envelope.tenant_id}:{business_id}".encode()


class KafkaEventProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        client_id: str,
        router: TopicRouter | None = None,
        retries: int = 3,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.router = router or TopicRouter()
        self.retries = retries
        self._producer: Any | None = None

    def _get_producer(self) -> Any:
        if self._producer is None:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                value_serializer=lambda value: value.encode("utf-8"),
                key_serializer=lambda value: value,
                acks="all",
                linger_ms=20,
                retries=5,
                max_in_flight_requests_per_connection=1,
            )
        return self._producer

    def publish(self, envelope: EventEnvelope, topic: str | None = None) -> dict[str, Any]:
        from platform_shared.tracing import inject_trace_context_into_headers, traced_span

        topic = topic or self.router.route(envelope.event_type)
        payload = envelope_to_json(envelope)
        key = self.router.partition_key(envelope)
        last_error: Exception | None = None

        # W3C trace-context propagation: injected as real Kafka message
        # headers (not the business-level trace_id/correlation_id already
        # in the event envelope, which this leaves untouched — see
        # platform_shared.tracing's module docstring for how the two
        # relate). A no-op dict pass-through when tracing is disabled.
        kafka_headers = [
            (k, v.encode("utf-8")) for k, v in inject_trace_context_into_headers({}).items()
        ]

        with traced_span(self.client_id, "kafka.publish", {"messaging.destination": topic, "messaging.system": "kafka"}):
            for attempt in range(1, self.retries + 1):
                try:
                    future = self._get_producer().send(topic, key=key, value=payload, headers=kafka_headers or None)
                    metadata = future.get(timeout=10)
                    return {
                        "topic": metadata.topic,
                        "partition": metadata.partition,
                        "offset": metadata.offset,
                        "attempt": attempt,
                    }
                except Exception as exc:  # pragma: no cover - exercised in integration.
                    last_error = exc
                    LOGGER.warning("Kafka publish failed", exc_info=exc, extra={"attempt": attempt})
                    time.sleep(min(2**attempt * 0.1, 2.0))

        raise RuntimeError(f"Kafka publish failed after {self.retries} attempts") from last_error

    def publish_dlq(self, envelope: EventEnvelope, error: str, failed_stage: str) -> dict[str, Any]:
        dlq_payload = {
            "failed_stage": failed_stage,
            "error": error,
            "original_event": envelope.model_dump(mode="json"),
        }
        dlq_envelope = envelope.model_copy(
            update={
                "event_type": EventType.SYSTEM_ALERT,
                "payload": {
                    "service_name": failed_stage,
                    "status": "dlq",
                    "error_count": 1,
                    "message": json.dumps(dlq_payload),
                },
                "source_service": failed_stage,
            }
        )
        return self.publish(dlq_envelope, topic=TopicNames.DLQ_EVENTS)


def create_consumer(
    *,
    bootstrap_servers: str,
    group_id: str,
    topics: Iterable[str],
    client_id: str,
) -> Any:
    from kafka import KafkaConsumer

    return KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        client_id=client_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: value.decode("utf-8"),
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
        max_poll_records=250,
        session_timeout_ms=30_000,
    )
