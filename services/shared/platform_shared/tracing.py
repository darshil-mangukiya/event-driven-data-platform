"""OpenTelemetry distributed tracing — real span creation and W3C trace
context propagation across the platform's actual request/Kafka boundary.

Target propagation path (live-verified, see
evidence/validation/opentelemetry-verification.md):

    SDK/ingestion request -> FastAPI ingestion span
        -> W3C traceparent injected into Kafka message headers
        -> processing-service consumer span (continues the same trace)
        -> PostgreSQL write, as a child span

Disabled by default (`OTEL_ENABLED` unset/false) — the platform runs
identically with or without a tracing backend; tracing is additive
observability, never a dependency the request path requires. When
enabled, spans export via OTLP/HTTP to a collector/backend (this project
live-tested against a real local Jaeger instance).
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any

_TRACER_PROVIDER_INITIALIZED = False


def tracing_enabled() -> bool:
    return os.getenv("OTEL_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def configure_tracing(service_name: str) -> None:
    """Idempotent — safe to call at every service's startup regardless of
    whether tracing is enabled; a no-op when it isn't.
    """
    global _TRACER_PROVIDER_INITIALIZED
    if not tracing_enabled() or _TRACER_PROVIDER_INITIALIZED:
        return

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    resource = Resource.create({SERVICE_NAME: service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _TRACER_PROVIDER_INITIALIZED = True


def instrument_fastapi_app(app: Any, service_name: str) -> None:
    if not tracing_enabled():
        return
    configure_tracing(service_name)
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)


def get_tracer(service_name: str):
    # Self-sufficient: works whether or not instrument_fastapi_app already
    # ran configure_tracing for this process (e.g. a standalone script
    # calling traced_span directly, or a call ordered before app startup).
    configure_tracing(service_name)
    from opentelemetry import trace

    return trace.get_tracer(service_name)


@contextmanager
def traced_span(service_name: str, span_name: str, attributes: dict[str, Any] | None = None, parent_context: Any = None):
    """A no-op context manager when tracing is disabled, so call sites
    (e.g. around a Kafka publish or a DB write) don't need their own
    `if tracing_enabled()` branch. Pass `parent_context` (from
    `extract_trace_context_from_headers`) to continue a trace propagated
    in from a Kafka message rather than starting a new, disconnected one.
    """
    if not tracing_enabled():
        yield None
        return
    tracer = get_tracer(service_name)
    if parent_context is not None:
        with tracer.start_as_current_span(span_name, context=parent_context, attributes=attributes or {}) as span:
            yield span
    else:
        with tracer.start_as_current_span(span_name, attributes=attributes or {}) as span:
            yield span


def inject_trace_context_into_headers(headers: dict[str, Any]) -> dict[str, Any]:
    """Injects the current span's W3C `traceparent` (and any baggage) into
    a plain dict of Kafka message headers, so a consumer on the other side
    of the topic can continue the same trace. No-op (returns headers
    unchanged) when tracing is disabled.
    """
    if not tracing_enabled():
        return headers
    from opentelemetry.propagate import inject

    carrier: dict[str, str] = {}
    inject(carrier)
    return {**headers, **carrier}


def extract_trace_context_from_headers(headers: dict[str, Any]):
    """The consumer-side half of inject_trace_context_into_headers — returns
    an OpenTelemetry Context to activate (via `opentelemetry.context.attach`)
    so a span started here is a child of the producer's span, not a new,
    disconnected trace. Returns None when tracing is disabled.
    """
    if not tracing_enabled():
        return None
    from opentelemetry.propagate import extract

    return extract(headers)
