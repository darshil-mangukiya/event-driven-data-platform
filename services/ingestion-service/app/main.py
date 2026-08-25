from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from platform_shared.auth import (
    TenantPrincipal,
    principal_from_authorization,
    require_tenant_access,
)
from platform_shared.config import service_settings
from platform_shared.kafka import KafkaEventProducer
from platform_shared.logging import configure_logging, get_logger
from platform_shared.metrics import (
    InMemoryServiceMetrics,
    MetricsMiddleware,
    record_event_published,
)
from platform_shared.schemas import EventEnvelope, EventType, build_envelope, idempotent_event_id
from platform_shared.tracing import instrument_fastapi_app
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response

SERVICE_NAME = "ingestion-service"
settings = service_settings(SERVICE_NAME)
configure_logging(SERVICE_NAME, settings.log_level)
logger = get_logger(__name__, SERVICE_NAME)
request_metrics = InMemoryServiceMetrics()
producer = KafkaEventProducer(settings.kafka_bootstrap_servers, settings.kafka_client_id)

app = FastAPI(
    title="Data Platform Ingestion Service",
    description="Validates tenant-scoped API and generated events, wraps them in platform envelopes, and publishes them to Kafka.",
    version="0.1.0",
)
app.add_middleware(MetricsMiddleware, metrics=request_metrics, service_name=SERVICE_NAME)
instrument_fastapi_app(app, SERVICE_NAME)  # no-op unless OTEL_ENABLED=true — see platform_shared.tracing


class IngestEventRequest(BaseModel):
    tenant_id: str = Field(min_length=2)
    event_type: EventType
    payload: dict[str, Any]
    source_service: str = Field(default="external-api", min_length=2)
    payload_version: int = Field(default=1, ge=1)
    event_timestamp: datetime | None = None
    event_id: str | None = Field(default=None, min_length=8)
    idempotency_key: str | None = Field(default=None, min_length=8, max_length=160)
    trace_id: str | None = Field(default=None, min_length=8)
    correlation_id: str | None = Field(default=None, min_length=8, max_length=160)
    causation_id: str | None = Field(default=None, min_length=8, max_length=160)


class IngestEventResponse(BaseModel):
    event_id: str
    tenant_id: str
    event_type: EventType
    topic: str
    partition: int
    offset: int
    trace_id: str
    correlation_id: str
    causation_id: str | None = None
    idempotency_key: str | None = None


class BatchIngestRequest(BaseModel):
    events: list[IngestEventRequest] = Field(min_length=1, max_length=500)


class BatchIngestResponse(BaseModel):
    accepted: int
    failed: int
    results: list[dict[str, Any]]


def get_principal(
    authorization: str | None = Header(default=None),
    x_tenant_id: str | None = Header(default=None),
    x_user_id: str | None = Header(default=None),
    x_user_role: str | None = Header(default=None),
) -> TenantPrincipal:
    try:
        return principal_from_authorization(
            authorization=authorization,
            tenant_id=x_tenant_id,
            user_id=x_user_id,
            role=x_user_role,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc


def _build_envelope(request: IngestEventRequest) -> EventEnvelope:
    event_id = request.event_id
    if event_id is None and request.idempotency_key:
        event_id = idempotent_event_id(
            tenant_id=request.tenant_id,
            event_type=request.event_type,
            source_service=request.source_service,
            idempotency_key=request.idempotency_key,
        )
    return build_envelope(
        tenant_id=request.tenant_id,
        event_type=request.event_type,
        payload=request.payload,
        source_service=request.source_service,
        event_id=event_id,
        event_timestamp=request.event_timestamp,
        payload_version=request.payload_version,
        trace_id=request.trace_id,
        correlation_id=request.correlation_id,
        causation_id=request.causation_id,
        idempotency_key=request.idempotency_key,
    )


async def _publish(request: IngestEventRequest) -> IngestEventResponse:
    envelope = _build_envelope(request)
    topic = producer.router.route(envelope.event_type)
    try:
        metadata = await run_in_threadpool(producer.publish, envelope, topic)
        record_event_published(SERVICE_NAME, topic, str(envelope.event_type), "success")
    except Exception:
        record_event_published(SERVICE_NAME, topic, str(envelope.event_type), "failed")
        raise
    logger.info(
        "published event",
        extra={"tenant_id": envelope.tenant_id, "trace_id": envelope.trace_id, "event_type": envelope.event_type},
    )
    return IngestEventResponse(
        event_id=envelope.event_id,
        tenant_id=envelope.tenant_id,
        event_type=envelope.event_type,
        topic=metadata["topic"],
        partition=metadata["partition"],
        offset=metadata["offset"],
        trace_id=envelope.trace_id,
        correlation_id=envelope.correlation_id or envelope.trace_id,
        causation_id=envelope.causation_id,
        idempotency_key=envelope.idempotency_key,
    )


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"service": SERVICE_NAME, "status": "ok", "environment": settings.environment}


@app.get("/metrics")
async def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/system/status")
async def system_status() -> dict[str, Any]:
    return {
        "service": SERVICE_NAME,
        "status": "ok",
        "kafka_bootstrap_servers": settings.kafka_bootstrap_servers,
        "request_metrics": request_metrics.snapshot(),
    }


@app.post("/events", response_model=IngestEventResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(
    request: IngestEventRequest,
    principal: TenantPrincipal = Depends(get_principal),
) -> IngestEventResponse:
    try:
        require_tenant_access(principal, request.tenant_id)
        return await _publish(request)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc


@app.post("/events/batch", response_model=BatchIngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_batch(
    request: BatchIngestRequest,
    principal: TenantPrincipal = Depends(get_principal),
) -> BatchIngestResponse:
    results: list[dict[str, Any]] = []
    accepted = 0
    failed = 0
    for event_request in request.events:
        try:
            require_tenant_access(principal, event_request.tenant_id)
            published = await _publish(event_request)
            accepted += 1
            results.append(published.model_dump(mode="json"))
        except Exception as exc:  # Batch ingestion reports individual failures.
            failed += 1
            results.append(
                {
                    "tenant_id": event_request.tenant_id,
                    "event_type": event_request.event_type,
                    "status": "failed",
                    "error": str(exc),
                }
            )
    return BatchIngestResponse(accepted=accepted, failed=failed, results=results)


@app.post("/generate/demo", response_model=BatchIngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def generate_demo_events(
    tenant_id: str,
    count: int = Query(default=50, ge=1, le=1_000),
    principal: TenantPrincipal = Depends(get_principal),
) -> BatchIngestResponse:
    try:
        require_tenant_access(principal, tenant_id)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    events = [_demo_event(tenant_id) for _ in range(count)]
    return await ingest_batch(BatchIngestRequest(events=events), principal)


def _demo_event(tenant_id: str) -> IngestEventRequest:
    event_type = random.choice(
        [
            EventType.ORDER_CREATED,
            EventType.PAYMENT_CAPTURED,
            EventType.PAYMENT_FAILED,
            EventType.USER_ACTIVITY,
            EventType.PRODUCT_INVENTORY_CHANGED,
            EventType.SYSTEM_HEALTH,
        ]
    )
    product_id = f"prod_{random.randint(1, 20):03d}"
    customer_id = f"cust_{random.randint(1, 500):05d}"
    order_id = f"ord_{uuid4().hex[:12]}"
    common = {
        EventType.ORDER_CREATED: {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": random.randint(1, 4),
            "unit_price": round(random.uniform(19, 499), 2),
            "discount_amount": random.choice([0, 5, 10, 25]),
            "status": "created",
            "channel": random.choice(["web", "mobile", "partner"]),
            "marketing_campaign_id": random.choice(["paid-search", "lifecycle", "affiliate", None]),
            "region": random.choice(["na", "emea", "apac"]),
        },
        EventType.PAYMENT_CAPTURED: {
            "payment_id": f"pay_{uuid4().hex[:12]}",
            "order_id": order_id,
            "customer_id": customer_id,
            "amount": round(random.uniform(19, 499), 2),
            "status": "captured",
            "payment_method": random.choice(["card", "wallet", "ach"]),
            "risk_score": round(random.uniform(0.01, 0.8), 3),
        },
        EventType.PAYMENT_FAILED: {
            "payment_id": f"pay_{uuid4().hex[:12]}",
            "order_id": order_id,
            "customer_id": customer_id,
            "amount": round(random.uniform(19, 499), 2),
            "status": "failed",
            "payment_method": random.choice(["card", "wallet", "ach"]),
            "failure_code": random.choice(["insufficient_funds", "risk_block", "processor_timeout"]),
            "risk_score": round(random.uniform(0.1, 0.99), 3),
        },
        EventType.USER_ACTIVITY: {
            "user_id": customer_id,
            "action": random.choice(["page_view", "checkout_started", "cart_updated", "feature_used"]),
            "session_id": f"sess_{random.randint(1, 5000):05d}",
            "page": random.choice(["/pricing", "/checkout", "/dashboard", "/products"]),
            "duration_seconds": random.randint(1, 600),
            "marketing_campaign_id": random.choice(["paid-search", "lifecycle", "affiliate", None]),
        },
        EventType.PRODUCT_INVENTORY_CHANGED: {
            "product_id": product_id,
            "sku": product_id.upper(),
            "name": f"Product {product_id}",
            "category": random.choice(["core", "growth", "enterprise", "marketplace"]),
            "price": round(random.uniform(19, 499), 2),
            "inventory_delta": random.randint(-20, 50),
            "active": True,
        },
        EventType.SYSTEM_HEALTH: {
            "service_name": random.choice(["ingestion-service", "processing-service", "analytics-service"]),
            "status": random.choice(["healthy", "degraded"]),
            "latency_ms": round(random.uniform(12, 350), 2),
            "error_count": random.randint(0, 5),
            "throughput_per_minute": round(random.uniform(500, 5000), 2),
            "kafka_lag": random.randint(0, 250),
            "cache_hit_rate": round(random.uniform(0.5, 0.99), 3),
        },
    }
    return IngestEventRequest(
        tenant_id=tenant_id,
        event_type=event_type,
        payload=common[event_type],
        source_service="demo-generator",
        event_timestamp=datetime.now(timezone.utc),
    )
