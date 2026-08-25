from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from platform_shared.schemas import EventEnvelope, EventType


@dataclass(frozen=True)
class MetricDelta:
    tenant_id: str
    metric_ts: datetime
    gross_revenue: float = 0
    net_revenue: float = 0
    order_count: int = 0
    units_sold: int = 0
    new_users: int = 0
    active_users: int = 0
    churn_signal_count: int = 0
    payment_success_count: int = 0
    payment_failure_count: int = 0
    marketing_spend: float = 0
    marketing_attributed_revenue: float = 0
    events_processed: int = 1


def event_hour(timestamp: datetime) -> datetime:
    timestamp = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
    return timestamp.replace(minute=0, second=0, microsecond=0)


def net_order_revenue(payload: dict[str, Any]) -> float:
    gross = float(payload["quantity"]) * float(payload["unit_price"])
    return max(gross - float(payload.get("discount_amount", 0)), 0)


def gross_order_revenue(payload: dict[str, Any]) -> float:
    return float(payload["quantity"]) * float(payload["unit_price"])


def processed_order_row(envelope: EventEnvelope) -> dict[str, Any]:
    payload = envelope.payload
    return {
        "tenant_id": envelope.tenant_id,
        "event_id": envelope.event_id,
        "order_id": payload["order_id"],
        "customer_id": payload["customer_id"],
        "product_id": payload["product_id"],
        "quantity": payload["quantity"],
        "unit_price": payload["unit_price"],
        "discount_amount": payload.get("discount_amount", 0),
        "gross_revenue": gross_order_revenue(payload),
        "net_revenue": net_order_revenue(payload),
        "currency": payload.get("currency", "USD"),
        "status": payload.get("status", "created"),
        "channel": payload.get("channel", "unknown"),
        "marketing_campaign_id": payload.get("marketing_campaign_id"),
        "region": payload.get("region"),
        "event_timestamp": envelope.event_timestamp,
    }


def processed_payment_row(envelope: EventEnvelope) -> dict[str, Any]:
    payload = envelope.payload
    return {
        "tenant_id": envelope.tenant_id,
        "event_id": envelope.event_id,
        "payment_id": payload["payment_id"],
        "order_id": payload["order_id"],
        "customer_id": payload["customer_id"],
        "amount": payload["amount"],
        "currency": payload.get("currency", "USD"),
        "status": payload["status"],
        "payment_method": payload.get("payment_method", "unknown"),
        "failure_code": payload.get("failure_code"),
        "risk_score": payload.get("risk_score"),
        "event_timestamp": envelope.event_timestamp,
    }


def processed_user_session_row(envelope: EventEnvelope) -> dict[str, Any]:
    payload = envelope.payload
    return {
        "tenant_id": envelope.tenant_id,
        "event_id": envelope.event_id,
        "user_id": payload["user_id"],
        "session_id": payload.get("session_id") or f"sessionless:{payload['user_id']}",
        "action": payload["action"],
        "page": payload.get("page"),
        "referrer": payload.get("referrer"),
        "duration_seconds": payload.get("duration_seconds", 0),
        "plan": payload.get("plan"),
        "marketing_campaign_id": payload.get("marketing_campaign_id"),
        "event_timestamp": envelope.event_timestamp,
    }


def product_state_row(envelope: EventEnvelope) -> dict[str, Any]:
    payload = envelope.payload
    return {
        "tenant_id": envelope.tenant_id,
        "event_id": envelope.event_id,
        "product_id": payload["product_id"],
        "sku": payload["sku"],
        "name": payload["name"],
        "category": payload["category"],
        "price": payload["price"],
        "inventory_delta": payload.get("inventory_delta", 0),
        "active": payload.get("active", True),
        "event_timestamp": envelope.event_timestamp,
    }


def service_health_row(envelope: EventEnvelope) -> dict[str, Any]:
    payload = envelope.payload
    return {
        "tenant_id": envelope.tenant_id,
        "event_id": envelope.event_id,
        "service_name": payload["service_name"],
        "status": payload["status"],
        "latency_ms": payload.get("latency_ms"),
        "error_count": payload.get("error_count", 0),
        "throughput_per_minute": payload.get("throughput_per_minute"),
        "kafka_lag": payload.get("kafka_lag"),
        "cache_hit_rate": payload.get("cache_hit_rate"),
        "message": payload.get("message"),
        "event_timestamp": envelope.event_timestamp,
    }


def metric_delta_for_event(envelope: EventEnvelope) -> MetricDelta:
    event_type = EventType(envelope.event_type)
    timestamp = event_hour(envelope.event_timestamp)
    payload = envelope.payload

    if event_type in {EventType.ORDER_CREATED, EventType.ORDER_UPDATED}:
        net_revenue = net_order_revenue(payload)
        campaign = payload.get("marketing_campaign_id")
        return MetricDelta(
            tenant_id=envelope.tenant_id,
            metric_ts=timestamp,
            gross_revenue=gross_order_revenue(payload),
            net_revenue=net_revenue,
            order_count=1,
            units_sold=int(payload["quantity"]),
            marketing_spend=3.50 if campaign else 0,
            marketing_attributed_revenue=net_revenue if campaign else 0,
        )

    if event_type in {EventType.PAYMENT_CAPTURED, EventType.PAYMENT_AUTHORIZED}:
        return MetricDelta(
            tenant_id=envelope.tenant_id,
            metric_ts=timestamp,
            payment_success_count=1,
        )

    if event_type == EventType.PAYMENT_FAILED:
        return MetricDelta(
            tenant_id=envelope.tenant_id,
            metric_ts=timestamp,
            payment_failure_count=1,
        )

    if event_type == EventType.USER_SIGNED_UP:
        return MetricDelta(
            tenant_id=envelope.tenant_id,
            metric_ts=timestamp,
            new_users=1,
            active_users=1,
        )

    if event_type == EventType.USER_ACTIVITY:
        return MetricDelta(
            tenant_id=envelope.tenant_id,
            metric_ts=timestamp,
            active_users=1,
        )

    if event_type == EventType.USER_CHURN_SIGNAL:
        return MetricDelta(
            tenant_id=envelope.tenant_id,
            metric_ts=timestamp,
            churn_signal_count=1,
        )

    return MetricDelta(tenant_id=envelope.tenant_id, metric_ts=timestamp)


def should_raise_risk_alert(envelope: EventEnvelope) -> bool:
    if EventType(envelope.event_type) != EventType.PAYMENT_FAILED:
        return False
    risk_score = envelope.payload.get("risk_score")
    return risk_score is not None and float(risk_score) >= 0.85

