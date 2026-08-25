# OpenTelemetry Runtime Result

Status: **EXECUTED AND VERIFIED LOCALLY**.

FastAPI instrumentation, trace/log correlation helpers, and W3C trace-context
injection/extraction for Kafka headers are covered by tests. A temporary local
Jaeger 1.60 all-in-one backend accepted OTLP/HTTP spans from the existing
instrumentation. One synthetic ingestion request produced trace
`33e828ea6a15e24d82e4be1050d94a96`: seven spans across `ingestion-service` and
`processing-service`, 257.610 ms aggregate span duration, and zero error spans.

The ingestion `POST /events` root, `kafka.publish`, and processing
`kafka.consume` spans share that trace ID. The publish and consume spans are
siblings beneath the HTTP root because the producer injects W3C headers before
entering its publish span; the shared trace nevertheless verifies context
continuity across Kafka. PostgreSQL is not instrumented, so this record does
not claim a database span. The temporary backend was removed after capture.

See `trace_summary.json` for the compact machine-readable record and
`TRACE_VALIDATION.md` for the reviewed boundary.
