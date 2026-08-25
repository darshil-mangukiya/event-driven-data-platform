# SLO and SLA Pack

This project defines platform SLOs so reliability expectations are explicit.

For the full SLI/SLO catalog (including streaming pipeline SLOs with
exact metric names, calculations, evaluation windows, and alert
mappings), see [docs/slo-catalog.md](slo-catalog.md).

## Service-Level Objectives

| Area | SLO | Measurement | Alert |
| --- | --- | --- | --- |
| Ingestion availability | 99.5% monthly | `/health` and publish success | Critical when service down for 2 minutes |
| Ingestion latency | p95 batch request under 500 ms locally | `platform_api_request_latency_seconds` | Warning above 1 second for 10 minutes |
| Analytics API latency | p95 under 300 ms for cached/hot queries | `platform_api_request_latency_seconds` | Warning above 750 ms for 10 minutes |
| Data freshness | Raw events arrive within 3 hours per active tenant | data quality freshness check | Critical freshness failure |
| DLQ rate | DLQ under 0.1% of processed events | Kafka processed/published counters and DLQ audit | Critical when DLQ growth persists |
| Data quality score | Tenant daily score >= 90 | `data_quality_score_daily` | Warning under 90, critical under 75 |
| Streaming processing lag | < 120s sustained | `cloudscale_stream_processing_lag_seconds` | Warning above 120s for 5 minutes |
| Streaming checkpoint freshness | < 5 minutes | `cloudscale_stream_checkpoint_age_seconds` | Critical when stale for 5 minutes |
| Reconciliation correctness | 100% of checks pass | `cloudscale_reconciliation_recent_failures` | Critical when any failure persists 15m |
| Cache dependency health | Redis available | `platform_cache_available{cache="redis"}` | Warning when unavailable for 2 minutes |

## SLA Framing

For local use, these are SLO targets, not contractual SLAs. In production, an SLA would depend on managed service choices, support hours, incident response staffing, and tenant contract tier.

## Incident Response Rules

1. Page on platform unavailability, sustained DLQ growth, or critical quality failure.
2. Triage health endpoints and Prometheus metrics first.
3. Check `pipeline_run_log`, `alerts`, and `data_quality_check_results`.
4. Pause replay until root cause is fixed.
5. Record replay attempts in `dlq_replay_audit`.
