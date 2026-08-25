from __future__ import annotations

from datetime import datetime, timezone

import pytest
from platform_shared.kafka import TopicRouter
from platform_shared.schemas import (
    EventEnvelope,
    EventType,
    build_envelope,
    envelope_from_json,
    envelope_to_json,
)
from pydantic import ValidationError


def test_order_event_envelope_validates_payload_and_round_trips_json() -> None:
    envelope = build_envelope(
        tenant_id="tenant_demo",
        event_type=EventType.ORDER_CREATED,
        source_service="test-suite",
        event_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        payload={
            "order_id": "ord_1",
            "customer_id": "cust_1",
            "product_id": "prod_1",
            "quantity": 2,
            "unit_price": 10.0,
            "discount_amount": 1.0,
            "status": "created",
        },
    )

    restored = envelope_from_json(envelope_to_json(envelope))

    assert restored.event_id == envelope.event_id
    assert restored.payload["currency"] == "USD"
    assert restored.payload["channel"] == "web"


def test_invalid_payload_is_rejected_before_kafka_publish() -> None:
    with pytest.raises(ValidationError):
        EventEnvelope(
            tenant_id="tenant_demo",
            event_type=EventType.ORDER_CREATED,
            source_service="test-suite",
            payload={
                "order_id": "ord_1",
                "customer_id": "cust_1",
                "product_id": "prod_1",
                "quantity": 0,
                "unit_price": 10.0,
            },
        )


def test_topic_router_uses_domain_topic_and_tenant_business_key() -> None:
    envelope = build_envelope(
        tenant_id="tenant_demo",
        event_type=EventType.PAYMENT_FAILED,
        source_service="test-suite",
        payload={
            "payment_id": "pay_1",
            "order_id": "ord_1",
            "customer_id": "cust_1",
            "amount": 99.0,
            "status": "failed",
            "failure_code": "risk_block",
        },
    )

    router = TopicRouter()

    assert router.route(envelope.event_type) == "platform.events.payments"
    assert router.partition_key(envelope) == b"tenant_demo:pay_1"
