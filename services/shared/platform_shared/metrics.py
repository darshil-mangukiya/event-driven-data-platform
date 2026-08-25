from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from prometheus_client import Counter, Gauge, Histogram
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

REQUEST_COUNT = Counter(
    "platform_api_requests_total",
    "HTTP requests handled by platform services.",
    ["service", "method", "path", "status_code"],
)
REQUEST_LATENCY = Histogram(
    "platform_api_request_latency_seconds",
    "HTTP request latency by service and route.",
    ["service", "method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
KAFKA_EVENTS_PUBLISHED = Counter(
    "platform_kafka_events_published_total",
    "Kafka events published by service, topic, event type, and status.",
    ["service", "topic", "event_type", "status"],
)
KAFKA_EVENTS_PROCESSED = Counter(
    "platform_kafka_events_processed_total",
    "Kafka events processed by service, event type, and status.",
    ["service", "event_type", "status"],
)
CACHE_EVENTS = Counter(
    "platform_cache_events_total",
    "Cache operations by service, cache name, and outcome.",
    ["service", "cache", "outcome"],
)
# Point-in-time availability, distinct from CACHE_EVENTS (a counter of past
# operations): 1 = the last operation against this cache succeeded, 0 = the
# last operation hit the outage fallback path (see
# platform_shared.cache.RedisCache._record_unavailable). Low cardinality —
# labeled only by service and cache name, never by key or error text.
CACHE_AVAILABILITY = Gauge(
    "platform_cache_available",
    "1 if the last cache operation succeeded, 0 if it hit the outage fallback path.",
    ["service", "cache"],
)
# Real Kafka consumer-group lag, computed from the consumer's own
# end_offsets()/position() (see record_consumer_lag) — this is the same
# signal KEDA's ScaledObject uses
# (evidence/validation/keda-autoscaling-live-verification.md),
# now also exposed via Prometheus so it's visible without a Kubernetes/KEDA
# deployment and alertable through the existing Prometheus/Grafana stack.
KAFKA_CONSUMER_LAG = Gauge(
    "platform_kafka_consumer_lag",
    "Consumer-group lag (high-water mark minus committed offset) per service, topic, and partition.",
    ["service", "topic", "partition"],
)


def record_consumer_lag(service_name: str, topic: str, partition: int, lag: int) -> None:
    KAFKA_CONSUMER_LAG.labels(service=service_name, topic=topic, partition=str(partition)).set(lag)


@dataclass
class InMemoryServiceMetrics:
    request_count: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    latency_ms_sum: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    error_count: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def observe(self, path: str, method: str, status_code: int, latency_ms: float) -> None:
        key = f"{method} {path}"
        self.request_count[key] += 1
        self.latency_ms_sum[key] += latency_ms
        if status_code >= 500:
            self.error_count[key] += 1

    def snapshot(self) -> dict[str, Any]:
        routes = []
        for key, count in self.request_count.items():
            avg_latency_ms = self.latency_ms_sum[key] / count if count else 0
            routes.append(
                {
                    "route": key,
                    "requests": count,
                    "avg_latency_ms": round(avg_latency_ms, 2),
                    "errors": self.error_count[key],
                }
            )
        return {"routes": sorted(routes, key=lambda route: route["route"])}


class MetricsMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: Any, metrics: InMemoryServiceMetrics, service_name: str = "unknown") -> None:
        super().__init__(app)
        self.metrics = metrics
        self.service_name = service_name

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        started = time.perf_counter()
        response = await call_next(request)
        latency_seconds = time.perf_counter() - started
        latency_ms = latency_seconds * 1000
        self.metrics.observe(
            path=request.url.path,
            method=request.method,
            status_code=response.status_code,
            latency_ms=latency_ms,
        )
        REQUEST_COUNT.labels(
            service=self.service_name,
            method=request.method,
            path=request.url.path,
            status_code=str(response.status_code),
        ).inc()
        REQUEST_LATENCY.labels(
            service=self.service_name,
            method=request.method,
            path=request.url.path,
        ).observe(latency_seconds)
        response.headers["X-Response-Time-Ms"] = f"{latency_ms:.2f}"
        return response


def record_event_published(service_name: str, topic: str, event_type: str, status: str) -> None:
    KAFKA_EVENTS_PUBLISHED.labels(
        service=service_name,
        topic=topic,
        event_type=event_type,
        status=status,
    ).inc()


def record_event_processed(service_name: str, event_type: str, status: str) -> None:
    KAFKA_EVENTS_PROCESSED.labels(
        service=service_name,
        event_type=event_type,
        status=status,
    ).inc()


def record_cache_event(service_name: str, cache_name: str, outcome: str) -> None:
    CACHE_EVENTS.labels(service=service_name, cache=cache_name, outcome=outcome).inc()


def record_cache_availability(service_name: str, cache_name: str, available: bool) -> None:
    CACHE_AVAILABILITY.labels(service=service_name, cache=cache_name).set(1 if available else 0)
