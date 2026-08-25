from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from app.worker import EventProcessor
from platform_shared.schemas import EventType, build_envelope


class FakeProcessingRepository:
    def __init__(self) -> None:
        self.raw_events: list[str] = []
        self.orders: list[dict] = []
        self.payments: list[dict] = []
        self.sessions: list[dict] = []
        self.products: list[dict] = []
        self.health: list[dict] = []
        self.metric_deltas: list[object] = []
        self.risk_alerts: list[str] = []

    async def write_raw_event(self, envelope) -> bool:
        self.raw_events.append(envelope.event_id)
        return True

    async def write_processed_order(self, row: dict) -> None:
        self.orders.append(row)

    async def write_processed_payment(self, row: dict) -> None:
        self.payments.append(row)

    async def write_user_session_event(self, row: dict) -> None:
        self.sessions.append(row)

    async def write_product_state(self, row: dict) -> None:
        self.products.append(row)

    async def write_service_health(self, row: dict) -> None:
        self.health.append(row)

    async def write_metric_delta(self, delta) -> None:
        self.metric_deltas.append(delta)

    async def write_risk_alert(self, envelope) -> None:
        self.risk_alerts.append(envelope.event_id)

    async def mark_event_processed(self, envelope) -> None:
        return None


@pytest.mark.integration
def test_order_event_flows_from_envelope_to_processed_and_metrics() -> None:
    repository = FakeProcessingRepository()
    processor = EventProcessor(repository)  # type: ignore[arg-type]
    envelope = build_envelope(
        tenant_id="tenant_demo",
        event_type=EventType.ORDER_CREATED,
        source_service="integration-test",
        event_timestamp=datetime(2026, 1, 1, 8, 15, tzinfo=timezone.utc),
        payload={
            "order_id": "ord_integration_1",
            "customer_id": "cust_1",
            "product_id": "prod_001",
            "quantity": 2,
            "unit_price": 50.0,
            "discount_amount": 5.0,
            "status": "created",
            "marketing_campaign_id": "paid-search",
        },
    )

    asyncio.run(processor.handle(envelope))

    assert repository.raw_events == [envelope.event_id]
    assert repository.orders[0]["net_revenue"] == 95.0
    assert repository.metric_deltas[0].order_count == 1
    assert repository.metric_deltas[0].marketing_attributed_revenue == 95.0


@pytest.mark.integration
def test_high_risk_payment_flow_creates_processed_payment_and_alert() -> None:
    repository = FakeProcessingRepository()
    processor = EventProcessor(repository)  # type: ignore[arg-type]
    envelope = build_envelope(
        tenant_id="tenant_demo",
        event_type=EventType.PAYMENT_FAILED,
        source_service="integration-test",
        payload={
            "payment_id": "pay_integration_1",
            "order_id": "ord_integration_1",
            "customer_id": "cust_1",
            "amount": 95.0,
            "status": "failed",
            "payment_method": "card",
            "failure_code": "risk_block",
            "risk_score": 0.92,
        },
    )

    asyncio.run(processor.handle(envelope))

    assert repository.payments[0]["status"] == "failed"
    assert repository.metric_deltas[0].payment_failure_count == 1
    assert repository.risk_alerts == [envelope.event_id]


@pytest.mark.integration
def test_duplicate_event_replay_skips_domain_writes_and_metrics() -> None:
    class DuplicateRepository(FakeProcessingRepository):
        async def write_raw_event(self, envelope) -> bool:
            self.raw_events.append(envelope.event_id)
            return False

    repository = DuplicateRepository()
    processor = EventProcessor(repository)  # type: ignore[arg-type]
    envelope = build_envelope(
        tenant_id="tenant_demo",
        event_type=EventType.ORDER_CREATED,
        source_service="integration-test",
        event_id="event-already-processed",
        payload={
            "order_id": "ord_duplicate",
            "customer_id": "cust_1",
            "product_id": "prod_001",
            "quantity": 2,
            "unit_price": 50.0,
            "discount_amount": 5.0,
            "status": "created",
        },
    )

    asyncio.run(processor.handle(envelope))

    assert repository.raw_events == [envelope.event_id]
    assert repository.orders == []
    assert repository.metric_deltas == []
