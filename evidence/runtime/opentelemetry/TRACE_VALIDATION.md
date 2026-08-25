# OpenTelemetry Trace Validation

Status: **EXECUTED AND VERIFIED LOCALLY**.

At `2026-09-03T17:45:16.956736Z`, one non-sensitive synthetic order submitted
to the ingestion API produced Jaeger trace
`33e828ea6a15e24d82e4be1050d94a96`. Jaeger recorded seven spans from two
services with zero error spans.

Observed key topology:

```text
POST /events (ingestion-service, HTTP 202)
|-- kafka.publish (ingestion-service, platform.events.orders)
`-- kafka.consume (processing-service, platform.events.orders)
```

The publish and consume spans are siblings because the existing producer
injects W3C context before entering the publish span. Their common trace and
root parent prove that the ingestion context survived the Kafka boundary.
Processing reported one consumed record. PostgreSQL client instrumentation is
not configured, so no database span is claimed.

The collector and backend were a temporary local Jaeger 1.60 all-in-one
container receiving OTLP/HTTP. It was not added to the default Compose stack
and was removed after evidence capture. This is one bounded synthetic local
trace, not production, scale, persistence, or cloud evidence.
