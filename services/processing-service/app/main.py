from __future__ import annotations

import asyncio
from typing import Any

from fastapi import FastAPI
from platform_shared.config import service_settings
from platform_shared.database import Postgres
from platform_shared.logging import configure_logging, get_logger
from platform_shared.metrics import InMemoryServiceMetrics, MetricsMiddleware
from platform_shared.tracing import instrument_fastapi_app
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from app.repository import ProcessingRepository
from app.worker import EventProcessor, KafkaProcessingWorker

SERVICE_NAME = "processing-service"
settings = service_settings(SERVICE_NAME)
configure_logging(SERVICE_NAME, settings.log_level)
logger = get_logger(__name__, SERVICE_NAME)
request_metrics = InMemoryServiceMetrics()

postgres = Postgres(settings.database_url)
repository = ProcessingRepository(postgres)
event_processor = EventProcessor(repository)
worker = KafkaProcessingWorker(
    bootstrap_servers=settings.kafka_bootstrap_servers,
    group_id=settings.kafka_consumer_group,
    client_id=settings.kafka_client_id,
    processor=event_processor,
)
worker_task: asyncio.Task[Any] | None = None

app = FastAPI(
    title="Data Platform Processing Service",
    description="Consumes Kafka events, writes processed tenant-scoped records, maintains aggregate tables, and records operational health.",
    version="0.1.0",
)
app.add_middleware(MetricsMiddleware, metrics=request_metrics, service_name=SERVICE_NAME)
instrument_fastapi_app(app, SERVICE_NAME)  # also configures the global TracerProvider the Kafka consumer's spans use — no-op unless OTEL_ENABLED=true


@app.on_event("startup")
async def startup() -> None:
    await postgres.connect()
    if settings.kafka_enable_consumer:
        global worker_task
        worker_task = asyncio.create_task(worker.start())
        logger.info("consumer task scheduled")


@app.on_event("shutdown")
async def shutdown() -> None:
    if worker_task:
        await worker.stop()
        worker_task.cancel()
    await postgres.close()


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"service": SERVICE_NAME, "status": "ok", "consumer_enabled": settings.kafka_enable_consumer}


@app.get("/metrics")
async def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/system/status")
async def system_status() -> dict[str, Any]:
    return {
        "service": SERVICE_NAME,
        "status": "running" if settings.kafka_enable_consumer else "api-only",
        "records_processed": worker.records_processed,
        "request_metrics": request_metrics.snapshot(),
    }
