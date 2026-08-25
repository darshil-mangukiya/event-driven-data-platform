# OpenTelemetry Distributed Tracing Verification

Status: **VERIFIED**

Date: 2026-08-22

Tracing is disabled by default. When `OTEL_ENABLED=true`, ingestion and
processing configure an OTLP/HTTP exporter, instrument FastAPI, and propagate
W3C `traceparent` headers through Kafka.

## Local Jaeger result

One trace contained:

```text
POST /events            ingestion-service server span
  -> kafka.publish      ingestion-service producer span
  -> kafka.consume      processing-service consumer span
```

The publish and consume spans shared a trace id and retained distinct service
resource attributes.

`tests/test_tracing.py` covers disabled behavior, span creation, idempotent
configuration, and inject/extract propagation. Business
`trace_id`/`correlation_id` fields remain separate from W3C tracing
context.

## Boundary

- PostgreSQL queries are not instrumented.
- Analytics, metadata, schema-registry, dashboards, and Spark are not wired to
  OpenTelemetry.
- Jaeger is a local verification backend and is not part of the default
  Grafana stack.
