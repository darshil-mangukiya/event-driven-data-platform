# SLI / SLO Catalog

Status: **local modeled SLOs** — targets based on local/demo defaults, not
calibrated against production traffic. This catalog defines what to
measure and what the acceptable ranges are for a local deployment; see
"Local vs. Production" below for what would change in a real environment.

## SLO Table

| # | SLO Name | SLI | Calculation | Source Metric | Target | Eval Window | Alert | Affected Component | Expected Response | Status |
|---|----------|-----|-------------|---------------|--------|-------------|-------|--------------------|-------------------|--------|
| 1 | Event-Processing Freshness | Time from event production to serving-table write | `cloudscale_serving_metrics_staleness_seconds{table="stream_window_metrics"}` | `cloudscale_serving_metrics_staleness_seconds` | < 3 hours | Rolling | ServingMetricsStale (> 10800s for 15m) | Streaming pipeline → PostgreSQL | Investigate streaming job health, checkpoint freshness, Kafka consumer lag | Local demo default |
| 2 | Streaming Availability | Fraction of time the streaming job processes batches | `rate(cloudscale_stream_batches_total{outcome="success"}[1h]) / rate(cloudscale_stream_batches_total[1h])` | `cloudscale_stream_batches_total` | > 99% of batches succeed | 1h rolling | StreamingSinkFailuresHigh (any failure after retries) | spark-streaming → PostgresSink | Check PostgreSQL health, inspect streaming_failures table, review sink retry logs | Local demo default |
| 3 | Serving Freshness (Async Path) | Age of newest `tenant_metrics_daily` row | `cloudscale_serving_metrics_staleness_seconds{table="tenant_metrics_daily"}` | `cloudscale_serving_metrics_staleness_seconds` | < 3 hours per active tenant | Rolling | ServingMetricsStale (> 10800s for 15m) | processing-service → PostgreSQL | Check processing-service health, Kafka consumer lag, pipeline_run_log | Local demo default |
| 4 | API Availability | Fraction of non-5xx responses | `1 - (sum(rate(platform_api_requests_total{status_code=~"5.."}[10m])) / sum(rate(platform_api_requests_total[10m])))` | `platform_api_requests_total` | > 99.5% | 10m rolling | AnalyticsServiceDown, ProcessingServiceDown (service unreachable for 2m) | analytics-service, ingestion-service | Restart service, check PostgreSQL/Redis dependency health | Local demo default |
| 5 | Analytics API Latency | p95 response time | `histogram_quantile(0.95, sum(rate(platform_api_request_latency_seconds_bucket{service="analytics-service"}[10m])) by (le))` | `platform_api_request_latency_seconds` | p95 < 300ms (cached), < 750ms (all) | 10m rolling | AnalyticsP95LatencyHigh (> 750ms for 10m) | analytics-service + Redis cache + PostgreSQL | Check cache hit rate, PostgreSQL query latency, connection pool health | Local demo default |
| 6 | Reconciliation Correctness | Fraction of passing reconciliation checks | `1 - (cloudscale_reconciliation_recent_failures / cloudscale_reconciliation_recent_checks)` | `cloudscale_reconciliation_recent_failures`, `cloudscale_reconciliation_recent_checks` | 100% of checks pass | 24h rolling | ReconciliationFailuresDetected (any failure persisting 15m) | reconcile_metrics.py → reconciliation_audit | Investigate mismatch deltas, run targeted backfill, re-reconcile | Local demo default |
| 7 | DLQ Rate | DLQ events as a fraction of received events | `sum(rate(cloudscale_stream_dlq_total[10m])) / sum(rate(cloudscale_stream_events_received_total[10m]))` | `cloudscale_stream_dlq_total`, `cloudscale_stream_events_received_total` | < 0.1% | 10m rolling | StreamingDlqGrowthHigh (> 50 in 10m) | Event validation → DLQ topic | Investigate upstream contract changes, check validation_reason distribution | Local demo default |
| 8 | Cache Dependency Health | Redis availability as seen by services | `platform_cache_available{cache="redis"}` | `platform_cache_available` | = 1 (healthy) | Point-in-time | RedisUnavailable (= 0 for 2m) | RedisCache → analytics-service rate limiting + caching | Redis is fail-open (services degrade, don't fail); restart Redis, monitor fallback rate | Local demo default |
| 9 | Checkpoint Freshness | Time since last streaming checkpoint commit | `time() - cloudscale_stream_checkpoint_age_seconds` | `cloudscale_stream_checkpoint_age_seconds` | < 5 minutes | Rolling | StreamingCheckpointStale (> 300s for 5m) | Structured Streaming checkpoint → HDFS/local | Investigate streaming query health, Spark driver logs | Local demo default |
| 10 | Processing Lag | Batch completion time minus max event_timestamp | `cloudscale_stream_processing_lag_seconds` | `cloudscale_stream_processing_lag_seconds` | < 120s | Rolling | StreamingProcessingLagHigh (> 120s for 5m) | Kafka → Spark → PostgreSQL | Scale streaming resources, check for slow sinks, review batch sizes | Local demo default |

## How These Are Measured

### Live-process metrics (scraped directly)

The Structured Streaming job (`spark-streaming` in docker-compose.yml)
exposes a Prometheus HTTP server on `:8007/metrics` — scraped by the
`spark-streaming` job in `monitoring/prometheus.yml`. These are real,
continuously-updated counters/gauges/histograms, not sampled or
batch-computed. Each service (ingestion, analytics, processing, metadata,
ops-console, demo-dashboard) also exposes `/metrics` via the shared
`MetricsMiddleware` in `platform_shared/metrics.py`.

### DB-derived gauges (ops-console)

Some SLIs require data that no single long-running process owns:
reconciliation outcomes, reliability exercise results, checkpoint
freshness as seen from outside the streaming job. These are computed by
`services/ops-console/app/observability.py::refresh_all()` right before
each `/metrics` scrape and exposed as `cloudscale_*` gauges on the same
Prometheus endpoint.

### What "local demo default" means

Every target in this catalog was set based on the local Docker Compose
environment's characteristics: single-node PostgreSQL, single Kafka
broker, local-mode Spark. Production calibration would involve:

1. Baseline measurement under realistic traffic volume
2. Stakeholder-agreed monthly error budgets in addition to rolling windows
3. Tier-specific SLAs for enterprise vs. self-serve tenants
4. Alert routing to on-call in addition to Prometheus rule evaluation

This catalog defines the *shape* of SLO management (what to measure, how
to calculate it, where the alert fires) so a production deployment can
fill in calibrated numbers without redesigning the instrumentation.

## Local vs. Production

| Aspect | Local | Production |
|--------|-------|------------|
| Thresholds | Demo defaults (this file) | Calibrated against real p99 baselines |
| Error budget | Not tracked | Monthly budget with burn-rate alerts |
| Alert routing | Prometheus rules only | PagerDuty/OpsGenie integration |
| Multi-tenant SLOs | Single tenant_demo | Per-tier SLO targets |
| Availability claim | Not claimed | Backed by uptime measurement |

## Relationship to Alert Rules

Every SLO above maps to one or more alert rules in
`monitoring/alert_rules.yml` or `monitoring/slo_rules.yml`. The alert
column in the table names the specific rule. See
`docs/observability.md` "Alert Inventory" for the full list.
