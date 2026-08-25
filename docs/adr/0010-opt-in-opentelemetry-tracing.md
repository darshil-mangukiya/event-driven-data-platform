# ADR 0010: Opt-In (Not Always-On) OpenTelemetry Tracing

## Status

Accepted

## Context

Distributed tracing across the Kafka boundary (ingestion-service →
processing-service, via W3C `traceparent` propagated in Kafka message
headers) is useful for debugging and was explicitly required.
It also introduces a real dependency (an OTLP collector such as Jaeger)
and, without care, would slow down or make network-flaky every
unit/integration test run and every environment that does not have a
collector available.

## Decision

Gate all tracing behind `OTEL_ENABLED` (default `false`).
`tracing.py`'s `get_tracer()` is self-initializing and safe to call
unconditionally; when disabled it returns a real no-op tracer with zero
network activity. `KafkaEventProducer.publish()` and the
processing-service consumer loop always call `traced_span()`/context
inject-extract, but those become inert no-ops when `OTEL_ENABLED=false`.
Only when explicitly enabled does the SDK initialize a real
`TracerProvider` with an OTLP/HTTP exporter pointed at
`OTEL_EXPORTER_OTLP_ENDPOINT`.

## Consequences

The full test suite and default `docker compose up` never depend on a
running collector — confirmed by the regression suite staying green with
tracing disabled by default. Turning tracing on for a real debugging
session or a demo is a single environment variable, and was live-verified
end-to-end against a real Jaeger instance
(`opentelemetry-verification.md`) — one continuous trace across the real
Kafka boundary. The cost is that trace context propagation code runs on
every publish/consume regardless of whether tracing is enabled (a cheap
no-op call), which was judged simpler and less error-prone than
conditionally skipping the instrumentation calls themselves.
