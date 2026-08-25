# Observability

The platform includes health endpoints, Prometheus metrics, Grafana
dashboards, alert rules, SLO definitions, and operational tables. This
document is the single reference for what is instrumented, how it is
collected, and where to find it.

## Health & Status Endpoints

Every service exposes:

- `GET /health` — process liveness (returns 200 when the service loop is
  running; used by Docker healthchecks and Kubernetes probes).
- `GET /system/status` — service-specific health, request metrics, cache
  stats, and downstream connectivity.
- `GET /metrics` — Prometheus text-format endpoint, scraped by the
  `prometheus` service in `docker-compose.yml`.

The Structured Streaming job (`spark-streaming`) exposes its own
`/metrics` on port 8007 via `prometheus_client.start_http_server` in the
Spark driver process — see `spark/streaming/metrics.py`.

## Metric Inventory

### Platform-wide (all HTTP services)

Defined in `services/shared/platform_shared/metrics.py`, emitted by the
`MetricsMiddleware` on every HTTP request:

| Metric | Type | Labels | Source |
|--------|------|--------|--------|
| `platform_api_requests_total` | Counter | service, method, path, status_code | MetricsMiddleware |
| `platform_api_request_latency_seconds` | Histogram | service, method, path | MetricsMiddleware |
| `platform_kafka_events_published_total` | Counter | service, topic, event_type, status | record_event_published() |
| `platform_kafka_events_processed_total` | Counter | service, event_type, status | record_event_processed() |
| `platform_cache_events_total` | Counter | service, cache, outcome | record_cache_event() |
| `platform_cache_available` | Gauge | service, cache | record_cache_availability() |
| `platform_kafka_consumer_lag` | Gauge | service, topic, partition | record_consumer_lag() |

### Structured Streaming (spark-streaming process)

Defined in `spark/streaming/metrics.py`, updated from real code paths in
`sinks.py`, `streaming_job.py`, and `dlq.py`:

| Metric | Type | Labels | Updated By |
|--------|------|--------|------------|
| `cloudscale_stream_events_received_total` | Counter | topic | streaming_job.py::received_batch |
| `cloudscale_stream_events_processed_total` | Counter | event_domain | streaming_job.py::aggregates_batch |
| `cloudscale_stream_events_failed_total` | Counter | reason | streaming_job.py::dlq_batch |
| `cloudscale_stream_events_duplicate_total` | Counter | event_domain | streaming_job.py::dedup_audit_batch |
| `cloudscale_stream_events_late_total` | Counter | classification | sinks.py::write_late_events |
| `cloudscale_stream_dlq_total` | Counter | reason | streaming_job.py::dlq_batch |
| `cloudscale_stream_batches_total` | Counter | query, outcome | streaming_job.py (each batch callback) |
| `cloudscale_stream_batch_duration_seconds` | Histogram | query | sinks.py::write_window_metrics |
| `cloudscale_stream_processing_lag_seconds` | Gauge | query | streaming_job.py::aggregates_batch (from query.lastProgress) |
| `cloudscale_stream_watermark_lag_seconds` | Gauge | query | streaming_job.py::aggregates_batch (from query.lastProgress) |
| `cloudscale_stream_sink_failures_total` | Counter | sink | sinks.py::_with_retries |
| `cloudscale_stream_checkpoint_age_seconds` | Gauge | query | sinks.py::write_checkpoint_audit |
| `cloudscale_stream_records_per_batch` | Histogram | query | sinks.py::write_window_metrics |
| `cloudscale_stream_postgres_available` | Gauge | (none) | sinks.py::_with_retries |

### DB-derived gauges (ops-console)

Defined in `services/ops-console/app/observability.py`, refreshed on
every `/metrics` scrape from database tables:

| Metric | Type | Labels | Source Table |
|--------|------|--------|--------------|
| `cloudscale_reliability_exercise_last_status` | Gauge | scenario_id | pipeline_run_log |
| `cloudscale_reliability_exercise_last_run_age_seconds` | Gauge | scenario_id | pipeline_run_log |
| `cloudscale_reconciliation_recent_failures` | Gauge | (none) | reconciliation_audit |
| `cloudscale_reconciliation_recent_checks` | Gauge | (none) | reconciliation_audit |
| `cloudscale_stream_checkpoint_freshness_seconds_db` | Gauge | query | streaming_checkpoint_audit |
| `cloudscale_stream_late_events_recent` | Gauge | classification | streaming_late_events |
| `cloudscale_serving_metrics_staleness_seconds` | Gauge | table | tenant_metrics_daily, stream_window_metrics |

### Total: 28 distinct metric families across 3 metric sources

All 28 are updated by runtime code paths rather than populated
only in tests.

## Label & Cardinality Strategy

**Rule**: every Prometheus label must come from a small, fixed,
enumerable set. High-cardinality identifiers — `event_id`, `trace_id`,
`order_id`, `customer_id`, `correlation_id`, raw error strings — are
**never** used as label values.

| Label | Max Cardinality | Used On |
|-------|-----------------|---------|
| `service` | 6 (ingestion, analytics, processing, metadata, ops-console, demo-dashboard) | platform_api_*, platform_kafka_*, platform_cache_* |
| `method` | 4 (GET, POST, PUT, DELETE) | platform_api_* |
| `path` | ~15 distinct API routes | platform_api_* |
| `status_code` | ~10 (200, 201, 202, 400, 401, 403, 404, 422, 429, 500) | platform_api_requests_total |
| `topic` | 7 (5 domain topics + retry + dlq) | cloudscale_stream_events_received_total |
| `event_domain` | 5 (orders, payments, users, products, system) | cloudscale_stream_events_processed_total, _duplicate_total |
| `reason` | ~8 validation reasons | cloudscale_stream_events_failed_total, _dlq_total |
| `classification` | 3 (on_time, late_accepted, late_rejected) | cloudscale_stream_events_late_total, _late_events_recent |
| `query` | 4 (aggregates, dlq, dedup-audit, received-metrics) | cloudscale_stream_batches_total, _batch_duration_seconds, etc. |
| `outcome` | 2 (success, failed) | cloudscale_stream_batches_total |
| `sink` | 4 (stream_window_metrics, streaming_late_events, streaming_checkpoint_audit, streaming_watermarks) | cloudscale_stream_sink_failures_total |
| `scenario_id` | 8 (the 8 reliability scenarios) | cloudscale_reliability_exercise_* |
| `table` | 2 (tenant_metrics_daily, stream_window_metrics) | cloudscale_serving_metrics_staleness_seconds |
| `cache` | 1 (redis) | platform_cache_available, platform_cache_events_total |
| `event_type` | ~12 (order.created, payment.processed, etc.) | platform_kafka_events_* |
| `status` | 3 (success, failed, skipped) | platform_kafka_events_* |

**Why `path` is acceptable despite being variable**: the platform has a
fixed set of API routes (not user-parameterized URLs). If a service
added dynamic path segments (e.g. `/orders/{order_id}`), the middleware
would need to normalize them — that's a documented future concern, not a
current problem.

**Why no `tenant_id` label**: tenant-scoped metrics would create O(tenants)
time series per metric family. Instead, per-tenant freshness and quality
are tracked in database tables (`tenant_metrics_daily`,
`data_quality_score_daily`) and surfaced via the ops-console / demo
dashboard — queryable by tenant without exploding Prometheus cardinality.

## Prometheus Configuration

`monitoring/prometheus.yml` defines scrape targets:

| Job | Target | Port | Metrics |
|-----|--------|------|---------|
| analytics-service | analytics-service:8000 | 8000 | platform_api_*, platform_cache_* |
| ingestion-service | ingestion-service:8000 | 8000 | platform_api_*, platform_kafka_events_published_* |
| processing-service | processing-service:8000 | 8000 | platform_api_*, platform_kafka_events_processed_* |
| metadata-service | metadata-service:8000 | 8000 | platform_api_* |
| demo-dashboard | demo-dashboard:8000 | 8000 | platform_api_* |
| ops-console | ops-console:8000 | 8000 | platform_api_*, cloudscale_reliability_*, cloudscale_reconciliation_*, cloudscale_stream_checkpoint_freshness_*, cloudscale_serving_* |
| spark-streaming | spark-streaming:8007 | 8007 | cloudscale_stream_* |

## Alert Inventory

### `monitoring/alert_rules.yml`

| Alert | Group | Severity | Condition | Threshold Rationale |
|-------|-------|----------|-----------|---------------------|
| ProcessingServiceDown | data-platform-local | critical | `up{job="processing-service"} == 0` for 2m | Basic liveness |
| AnalyticsServiceDown | data-platform-local | critical | `up{job="analytics-service"} == 0` for 2m | Basic liveness |
| StreamingProcessingLagHigh | cloudscale-streaming | warning | `cloudscale_stream_processing_lag_seconds > 120` for 5m | 4× default 30s trigger interval — single slow batch shouldn't page |
| StreamingDlqGrowthHigh | cloudscale-streaming | warning | `increase(cloudscale_stream_dlq_total[10m]) > 50` | Sustained burst suggests upstream contract break |
| StreamingSinkFailuresHigh | cloudscale-streaming | critical | `increase(cloudscale_stream_sink_failures_total[5m]) > 0` | Any failure after retries is worth paging |
| StreamingCheckpointStale | cloudscale-streaming | critical | `time() - cloudscale_stream_checkpoint_age_seconds > 300` for 5m | 10× trigger interval — query is stuck, not slow |
| StreamingPostgresUnavailable | cloudscale-streaming | critical | `cloudscale_stream_postgres_available == 0` for 2m | Gives retry+backoff room before paging |
| StreamingExcessiveLateRejectedRate | cloudscale-streaming | warning | `rate(cloudscale_stream_events_late_total{classification="late_rejected"}[15m]) > 0.5` for 10m | Sustained rate suggests systemic clock skew |
| StreamingExcessiveValidationFailureRate | cloudscale-streaming | warning | `rate(cloudscale_stream_events_failed_total[10m]) > 1` for 10m | 1/s sustained means upstream contract break |
| RedisUnavailable | cloudscale-dependencies | warning | `platform_cache_available{cache="redis"} == 0` for 2m | Warning, not critical — services fail open |
| OpsConsoleDown | cloudscale-dependencies | warning | `up{job="ops-console"} == 0` for 2m | DB-derived gauges stop updating |
| ReconciliationFailuresDetected | cloudscale-reliability | critical | `cloudscale_reconciliation_recent_failures > 0` for 15m | Any mismatch is worth investigating |
| StreamingCheckpointStaleFromDatabase | cloudscale-reliability | warning | `cloudscale_stream_checkpoint_freshness_seconds_db > 600` for 5m | Independent of streaming process liveness |
| ServingMetricsStale | cloudscale-reliability | warning | `cloudscale_serving_metrics_staleness_seconds > 10800` for 15m | Matches 3-hour freshness SLO |

### `monitoring/slo_rules.yml`

| Alert | Condition |
|-------|-----------|
| AnalyticsP95LatencyHigh | p95 > 750ms for 10m |
| IngestionPublishFailures | Any failed publish for 5m |
| ProcessingFailures | Any processing failure for 5m |
| CacheHitRateLow | Hit rate < 50% for 15m |

## Grafana Dashboard

`monitoring/grafana_dashboard.json` — provisioned automatically into
Grafana via `monitoring/grafana/provisioning/`. Panels are organized
into rows:

1. **Platform Overview** — API latency, Kafka events, cache hit rate,
   request rate, error rate
2. **Structured Streaming Pipeline** — event throughput, processing lag,
   micro-batch duration, events processed by domain, validation failure
   rate, batches processed, records per batch
3. **Late Events & Deduplication** — late events by classification,
   recent late events (DB), duplicate events
4. **DLQ & Sink Health** — DLQ volume, sink failures, PostgreSQL health
5. **Cache & Dependency Health** — Redis availability + fallback rate,
   cache operations by outcome, Kafka publish health
6. **Reconciliation & Reliability** — reconciliation health,
   reliability exercise results, checkpoint freshness, serving
   staleness, pipeline run outcomes

**Live dashboard status**: API AND QUERY VERIFIED, VISUAL CAPTURE NOT
RECORDED — Grafana loaded the provisioned datasource and dashboard during the
bounded local run. Prometheus accepted all 33 panel expressions and 25 returned
non-empty data. The JSON remains structurally validated by
`tests/test_observability.py`; no browser screenshot or claim that every panel
contained data is made.

## Reliability → Detection Mapping

Each reliability exercise (see `docs/reliability.md`) maps to a specific
metric/alert that would detect the corresponding failure in a running
system:

| Reliability Exercise | Injected Failure | Detection Metric | Alert Rule |
|---------------------|------------------|------------------|------------|
| `poison-event` | Unsupported payload_version, missing fields | `cloudscale_stream_events_failed_total{reason="unsupported_payload_version"}` | StreamingExcessiveValidationFailureRate, StreamingDlqGrowthHigh |
| `duplicate-event` | Same (tenant_id, event_id) published twice | `cloudscale_stream_events_duplicate_total` | (informational — duplicates are suppressed, not alerted on) |
| `late-event` | Events past watermark thresholds | `cloudscale_stream_events_late_total{classification="late_rejected"}` | StreamingExcessiveLateRejectedRate |
| `consumer-lag` | Processing falling behind real-time | `cloudscale_stream_processing_lag_seconds` | StreamingProcessingLagHigh |
| `db-outage` | PostgreSQL unreachable | `cloudscale_stream_postgres_available`, `cloudscale_stream_sink_failures_total` | StreamingPostgresUnavailable, StreamingSinkFailuresHigh |
| `redis-outage` | Redis unreachable | `platform_cache_available{cache="redis"}`, `platform_cache_events_total{outcome="unavailable"}` | RedisUnavailable |
| `reconciliation-mismatch` | Injected metric delta | `cloudscale_reconciliation_recent_failures` | ReconciliationFailuresDetected |
| `consumer-interruption` | Streaming query stop/restart | `cloudscale_stream_checkpoint_age_seconds` (gap during outage) | StreamingCheckpointStale |

This table defines the intended **failure injection → telemetry signal →
alert/detection → recovery → validation** mapping. Executed local scenarios and
their boundaries are listed in `evidence/runtime/reliability/result.md`; other
paths remain deterministic test evidence. See `docs/reliability.md` for the
recovery and validation steps.

## Evidence Tables

Database tables that store operational evidence beyond Prometheus
time-series:

| Table | Purpose | Written By |
|-------|---------|------------|
| `api_usage_log` | API traffic, latency, status, cache status, role, trace metadata | analytics-service |
| `service_health_metrics` | Service-level health summaries | health check scripts |
| `pipeline_run_log` | Job/pipeline run status (including reliability exercise outcomes) | runner.py, Airflow DAGs |
| `dlq_replay_audit` | DLQ replay attempts and outcomes | dlq_tool.py |
| `data_quality_check_results` | Check-level quality outcomes | run_data_quality_checks.py |
| `data_quality_score_daily` | Tenant quality score snapshots | run_data_quality_checks.py |
| `benchmark_run_results` | Local benchmark summaries | benchmark_report.py |
| `reconciliation_audit` | Reconciliation check results | reconcile_metrics.py |
| `stream_processing_runs` | Streaming job lifecycle | PostgresSink |
| `stream_window_metrics` | Windowed aggregation results | PostgresSink |
| `streaming_watermarks` | Watermark progression | PostgresSink |
| `streaming_checkpoint_audit` | Checkpoint commit records | PostgresSink |
| `streaming_failures` | Sink write failures | PostgresSink.log_failure |
| `streaming_late_events` | Late/duplicate event audit | PostgresSink |

## Production Extension Path

Production would add:

- **Distributed tracing**: OpenTelemetry SDK → Jaeger/Tempo, with trace
  context propagated through Kafka headers and Spark batch callbacks
- **Centralized logging**: Structured JSON logs → Loki/CloudWatch/ELK
- **Alert routing**: PagerDuty/OpsGenie integration for critical alerts
- **SLO error budgets**: Monthly burn-rate tracking per SLO
- **Multi-tenant dashboards**: Per-tenant Grafana variables (feasible
  because per-tenant metrics are in database tables, not Prometheus
  labels — see "Label & Cardinality Strategy")
- **Dashboard-as-code**: Grafonnet/Terraform-managed dashboards
