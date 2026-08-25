from __future__ import annotations

from datetime import datetime, timezone

from app.processors import (
    metric_delta_for_event,
    net_order_revenue,
    processed_order_row,
    should_raise_risk_alert,
)
from platform_shared.schemas import EventType, build_envelope


def test_order_processing_computes_revenue_and_metric_delta() -> None:
    envelope = build_envelope(
        tenant_id="tenant_demo",
        event_type=EventType.ORDER_CREATED,
        source_service="test-suite",
        event_timestamp=datetime(2026, 1, 2, 3, 42, tzinfo=timezone.utc),
        payload={
            "order_id": "ord_1",
            "customer_id": "cust_1",
            "product_id": "prod_1",
            "quantity": 3,
            "unit_price": 25.0,
            "discount_amount": 5.0,
            "status": "created",
            "marketing_campaign_id": "paid-search",
        },
    )

    row = processed_order_row(envelope)
    delta = metric_delta_for_event(envelope)

    assert net_order_revenue(envelope.payload) == 70.0
    assert row["gross_revenue"] == 75.0
    assert row["net_revenue"] == 70.0
    assert delta.metric_ts.hour == 3
    assert delta.order_count == 1
    assert delta.units_sold == 3
    assert delta.marketing_spend == 3.50
    assert delta.marketing_attributed_revenue == 70.0


def test_high_risk_failed_payment_creates_alert_signal() -> None:
    envelope = build_envelope(
        tenant_id="tenant_demo",
        event_type=EventType.PAYMENT_FAILED,
        source_service="test-suite",
        payload={
            "payment_id": "pay_1",
            "order_id": "ord_1",
            "customer_id": "cust_1",
            "amount": 150.0,
            "status": "failed",
            "failure_code": "risk_block",
            "risk_score": 0.91,
        },
    )

    delta = metric_delta_for_event(envelope)

    assert delta.payment_failure_count == 1
    assert should_raise_risk_alert(envelope) is True

