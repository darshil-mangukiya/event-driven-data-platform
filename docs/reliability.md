# Reliability Exercises

`reliability/` provides repeatable local failure simulations. Generated
artifacts are exercise records, not production incident records.

## Status model

Each step uses one of four statuses:

- `verified`: the step executed and its assertion passed.
- `simulated`: expected behavior was computed without the runtime dependency.
- `not_run`: a required dependency was unavailable.
- `failed`: the step executed and its assertion failed.

A scenario fails if any step fails. A run containing only skipped steps is
`not_run`.

## Scenarios

| Scenario | CLI id | Check | Runtime dependency |
| --- | --- | --- | --- |
| Poison event | `poison-event` | Unsupported versions and missing fields are classified and routed | Kafka optional |
| Duplicate event | `duplicate-event` | Same-batch flags and cross-batch watermark deduplication | Spark local |
| Late event | `late-event` | Accepted and rejected lateness thresholds | Spark local |
| Consumer lag | `consumer-lag` | Kafka group lag or alert-threshold path | Kafka optional |
| PostgreSQL outage | `db-outage` | Bounded sink retries and `SinkError` | None; TEST-NET endpoint |
| Redis outage | `redis-outage` | Cache miss/no-op/fail-open behavior | None; TEST-NET endpoint |
| Reconciliation mismatch | `reconciliation-mismatch` | Mismatch status and exact deltas | None |
| Consumer interruption | `consumer-interruption` | Restart from the same Spark checkpoint | Spark local |

## Commands

```bash
python -m platform_cli reliability list
python -m platform_cli reliability run poison-event
python -m platform_cli reliability run --all
make reliability-test
make reliability-all
```

Each run writes `scenario.json`, `incident_report.md`, `metrics.json`,
`validation.json`, and `remediation.md` under
`artifacts/reliability/<run_id>/`. That directory is ignored by Git.

## Detection mapping

| Exercise | Metric | Alert |
| --- | --- | --- |
| `poison-event` | `cloudscale_stream_events_failed_total` | `StreamingExcessiveValidationFailureRate`, `StreamingDlqGrowthHigh` |
| `duplicate-event` | `cloudscale_stream_events_duplicate_total` | Informational |
| `late-event` | `cloudscale_stream_events_late_total` | `StreamingExcessiveLateRejectedRate` |
| `consumer-lag` | `cloudscale_stream_processing_lag_seconds` | `StreamingProcessingLagHigh` |
| `db-outage` | `cloudscale_stream_postgres_available`, sink failures | `StreamingPostgresUnavailable`, `StreamingSinkFailuresHigh` |
| `redis-outage` | `platform_cache_available` | `RedisUnavailable` |
| `reconciliation-mismatch` | `cloudscale_reconciliation_recent_failures` | `ReconciliationFailuresDetected` |
| `consumer-interruption` | `cloudscale_stream_checkpoint_age_seconds` | `StreamingCheckpointStale` |

Exercise outcomes are stored in `pipeline_run_log`, exported by the ops
console, scraped by Prometheus, and displayed in Grafana.

## Boundaries

- Kafka publication and consumer-group measurements require a reachable broker.
- PostgreSQL and Redis outage exercises use RFC 5737 TEST-NET endpoints; they
  exercise client failure handling without stopping containers.
- Consumer interruption uses `query.stop()` and a new query against the same
  checkpoint. It does not kill the Spark driver process.
- The file sink is used for checkpoint recovery because Spark's memory sink
  does not support recovery from a checkpoint.

Tests live under `tests/reliability/`; Spark-specific checks are under
`tests/streaming/`.
