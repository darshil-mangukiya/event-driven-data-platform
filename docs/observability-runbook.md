# Observability Runbook

## Health Checks

- `GET /health` for process liveness.
- `GET /system/status` for service-specific health, request metrics, cache stats, and latest platform status.

## Important Signals

- API latency and error rate.
- Prometheus metrics from `/metrics`, including request count, latency histograms, Kafka event counters, and cache operation counters.
- Kafka consumer lag.
- DLQ growth.
- Processing records per minute.
- Spark job failures.
- Pipeline run failures.
- Redis cache hit rate.
- Payment failure spikes.
- Tenant health score decline.

## Triage Steps

1. Check service `/health` and container status.
2. Inspect `service_health_metrics` for latency, error count, and Kafka lag.
3. Check `pipeline_run_log` for failed batch or Spark jobs.
4. Check `alerts` for open platform or risk alerts.
5. Inspect Kafka DLQ records and original event payloads.
6. Replay DLQ records only after fixing the root cause and confirming idempotency.

## Prometheus Metrics

- `platform_api_requests_total`
- `platform_api_request_latency_seconds`
- `platform_kafka_events_published_total`
- `platform_kafka_events_processed_total`
- `platform_cache_events_total`

## Evidence Tables

- `api_usage_log`: analytics API traffic, latency, status, and cache status.
- `dlq_replay_audit`: replay attempts and outcomes.
- `data_quality_check_results`: check-level quality outcomes.
- `data_quality_score_daily`: tenant quality score snapshots.
- `benchmark_run_results`: local benchmark summaries.

## Common Failures

- Kafka unavailable: ingestion publish fails, processing lag grows.
- Postgres unavailable: processing cannot commit events, analytics API fails.
- Redis unavailable: analytics can still query Postgres if cache fallback is added; current MVP surfaces Redis errors.
- Schema drift: ingestion rejects invalid payloads before Kafka.
- Hot tenant: query latency grows for one tenant, requiring partitioning, pre-aggregation, or tenant isolation.
