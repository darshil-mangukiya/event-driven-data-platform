# Observability

The platform records observability at three layers:

- Service layer: health endpoints, JSON logs, request timing middleware, API usage table scaffold.
- Pipeline layer: `pipeline_run_log`, Kafka consumer lag notes, Spark job status records, DLQ and retry topics.
- Business reliability layer: `alerts`, payment-risk events, tenant health score, failed event counts, and service health summaries.

The Prometheus config is intentionally lightweight for local development. Production should add Kafka exporter, Postgres exporter, Redis exporter, OpenTelemetry traces, structured log shipping, and alert routing through PagerDuty or Opsgenie.

