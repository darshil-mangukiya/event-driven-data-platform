from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class EventType(str, Enum):
    ORDER_CREATED = "order.created"
    ORDER_UPDATED = "order.updated"
    PAYMENT_AUTHORIZED = "payment.authorized"
    PAYMENT_CAPTURED = "payment.captured"
    PAYMENT_FAILED = "payment.failed"
    USER_SIGNED_UP = "user.signed_up"
    USER_ACTIVITY = "user.activity"
    USER_CHURN_SIGNAL = "user.churn_signal"
    PRODUCT_UPSERTED = "product.upserted"
    PRODUCT_INVENTORY_CHANGED = "product.inventory_changed"
    SYSTEM_HEALTH = "system.health"
    SYSTEM_ALERT = "system.alert"


class EventEnvelope(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str = Field(min_length=2, max_length=80)
    event_type: EventType
    event_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_service: str = Field(min_length=2, max_length=120)
    payload_version: int = Field(default=1, ge=1)
    payload: dict[str, Any]
    trace_id: str = Field(default_factory=lambda: str(uuid4()))
    correlation_id: str | None = None
    causation_id: str | None = None
    idempotency_key: str | None = None
    schema_ref: str | None = None

    @field_validator("event_timestamp", mode="before")
    @classmethod
    def ensure_timezone(cls, value: datetime | str) -> datetime | str:
        if isinstance(value, datetime) and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    @field_validator("tenant_id", "source_service", "trace_id", "correlation_id", "causation_id", "idempotency_key")
    @classmethod
    def strip_identity_fields(cls, value: str | None) -> str | None:
        return value.strip() if isinstance(value, str) else value

    @model_validator(mode="after")
    def validate_payload_contract(self) -> EventEnvelope:
        if self.correlation_id is None:
            self.correlation_id = self.trace_id
        if self.idempotency_key is None:
            self.idempotency_key = self.event_id
        self.payload = validate_event_payload(self.event_type, self.payload)
        return self


class OrderPayload(BaseModel):
    order_id: str
    customer_id: str
    product_id: str
    quantity: int = Field(ge=1)
    unit_price: float = Field(ge=0)
    discount_amount: float = Field(default=0, ge=0)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    status: str = Field(default="created")
    channel: str = Field(default="web")
    marketing_campaign_id: str | None = None
    region: str | None = None


class PaymentPayload(BaseModel):
    payment_id: str
    order_id: str
    customer_id: str
    amount: float = Field(ge=0)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    status: str
    payment_method: str = Field(default="card")
    failure_code: str | None = None
    risk_score: float | None = Field(default=None, ge=0, le=1)


class UserPayload(BaseModel):
    user_id: str
    action: str
    session_id: str | None = None
    page: str | None = None
    referrer: str | None = None
    duration_seconds: int = Field(default=0, ge=0)
    plan: str | None = None
    marketing_campaign_id: str | None = None


class ProductPayload(BaseModel):
    product_id: str
    sku: str
    name: str
    category: str
    price: float = Field(ge=0)
    inventory_delta: int = 0
    active: bool = True


class SystemPayload(BaseModel):
    service_name: str
    status: str
    latency_ms: float | None = Field(default=None, ge=0)
    error_count: int = Field(default=0, ge=0)
    throughput_per_minute: float | None = Field(default=None, ge=0)
    kafka_lag: int | None = Field(default=None, ge=0)
    cache_hit_rate: float | None = Field(default=None, ge=0, le=1)
    message: str | None = None


PAYLOAD_MODELS: dict[EventType, type[BaseModel]] = {
    EventType.ORDER_CREATED: OrderPayload,
    EventType.ORDER_UPDATED: OrderPayload,
    EventType.PAYMENT_AUTHORIZED: PaymentPayload,
    EventType.PAYMENT_CAPTURED: PaymentPayload,
    EventType.PAYMENT_FAILED: PaymentPayload,
    EventType.USER_SIGNED_UP: UserPayload,
    EventType.USER_ACTIVITY: UserPayload,
    EventType.USER_CHURN_SIGNAL: UserPayload,
    EventType.PRODUCT_UPSERTED: ProductPayload,
    EventType.PRODUCT_INVENTORY_CHANGED: ProductPayload,
    EventType.SYSTEM_HEALTH: SystemPayload,
    EventType.SYSTEM_ALERT: SystemPayload,
}


EVENT_DOMAIN: dict[EventType, str] = {
    EventType.ORDER_CREATED: "orders",
    EventType.ORDER_UPDATED: "orders",
    EventType.PAYMENT_AUTHORIZED: "payments",
    EventType.PAYMENT_CAPTURED: "payments",
    EventType.PAYMENT_FAILED: "payments",
    EventType.USER_SIGNED_UP: "users",
    EventType.USER_ACTIVITY: "users",
    EventType.USER_CHURN_SIGNAL: "users",
    EventType.PRODUCT_UPSERTED: "products",
    EventType.PRODUCT_INVENTORY_CHANGED: "products",
    EventType.SYSTEM_HEALTH: "system",
    EventType.SYSTEM_ALERT: "system",
}


class TopicNames:
    ORDER_EVENTS: ClassVar[str] = "platform.events.orders"
    PAYMENT_EVENTS: ClassVar[str] = "platform.events.payments"
    USER_EVENTS: ClassVar[str] = "platform.events.users"
    PRODUCT_EVENTS: ClassVar[str] = "platform.events.products"
    SYSTEM_EVENTS: ClassVar[str] = "platform.events.system"
    RETRY_EVENTS: ClassVar[str] = "platform.events.retry"
    DLQ_EVENTS: ClassVar[str] = "platform.events.dlq"


def validate_event_payload(event_type: EventType | str, payload: dict[str, Any]) -> dict[str, Any]:
    event_type = EventType(event_type)
    payload_model = PAYLOAD_MODELS[event_type]
    return payload_model(**payload).model_dump()


def build_envelope(
    *,
    tenant_id: str,
    event_type: EventType | str,
    payload: dict[str, Any],
    source_service: str,
    event_id: str | None = None,
    event_timestamp: datetime | None = None,
    payload_version: int = 1,
    trace_id: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    idempotency_key: str | None = None,
    schema_ref: str | None = None,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id or str(uuid4()),
        tenant_id=tenant_id,
        event_type=EventType(event_type),
        payload=payload,
        source_service=source_service,
        event_timestamp=event_timestamp or datetime.now(timezone.utc),
        payload_version=payload_version,
        trace_id=trace_id or str(uuid4()),
        correlation_id=correlation_id,
        causation_id=causation_id,
        idempotency_key=idempotency_key,
        schema_ref=schema_ref,
    )


def idempotent_event_id(
    *,
    tenant_id: str,
    event_type: EventType | str,
    source_service: str,
    idempotency_key: str,
) -> str:
    raw_key = "|".join(
        [
            tenant_id.strip(),
            EventType(event_type).value,
            source_service.strip(),
            idempotency_key.strip(),
        ]
    )
    digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:32]
    return f"idem_{digest}"


def envelope_to_json(envelope: EventEnvelope) -> str:
    return envelope.model_dump_json()


def envelope_from_json(raw: str | bytes) -> EventEnvelope:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return EventEnvelope.model_validate_json(raw)
