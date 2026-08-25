# Runbook

This runbook summarizes local operational workflows and recovery assumptions. Production RPO/RTO values would need stakeholder approval and restore drills.

## First Triage

1. Check service `/health` endpoints and container status.
2. Check `/system/status` for service-level latency, cache, and pipeline summaries.
3. Inspect `service_health_metrics`, `pipeline_run_log`, `alerts`, and `api_usage_log`.
4. Check Kafka topic health, consumer lag, and DLQ volume.
5. Run data quality and reconciliation scripts for affected tenants.
6. Replay or backfill only after the root cause is understood and idempotency has been checked.

## Useful Commands

```bash
PYTHONPATH=.:services/shared python -m platform_cli --pretty health check --dry-run
PYTHONPATH=.:services/shared python -m platform_cli --pretty ops reconciliation --tenant-id tenant_demo --dry-run
PYTHONPATH=services/shared python scripts/run_data_quality_checks.py --tenant-id tenant_demo --dry-run --pretty
```

## Docker Verification

Verify Docker Compose syntax and runtime health on a Docker-enabled machine:

```bash
docker compose config
docker compose -f docker-compose.yml up --build
python scripts/docker_smoke_check.py
```

Optional Airflow verification:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml config
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up --build
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-webserver airflow dags list
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-webserver airflow dags test cloudscale_operational_checks 2026-06-01
python scripts/docker_smoke_check.py --include-airflow
```

Do not claim Docker startup validation until these commands pass locally. Airflow is optional local orchestration for finite workflows; Kafka streaming remains service-driven.

## Streaming Pipeline Triage

If a streaming alert fires (StreamingProcessingLagHigh,
StreamingSinkFailuresHigh, StreamingCheckpointStale, etc.):

1. Check the streaming job's health:
   ```bash
   # Is the spark-streaming container running?
   docker compose --profile streaming ps spark-streaming
   # Check spark-streaming logs for exceptions
   docker compose --profile streaming logs spark-streaming --tail=100
   ```

2. Check the streaming metrics endpoint:
   ```bash
   curl -s http://localhost:8007/metrics | grep cloudscale_stream
   ```

3. Check checkpoint freshness:
   ```bash
   # Live process gauge
   curl -s http://localhost:8007/metrics | grep checkpoint_age
   # Database view (independent of process liveness)
   curl -s http://localhost:8006/metrics | grep checkpoint_freshness
   ```

4. Check sink health:
   ```bash
   curl -s http://localhost:8007/metrics | grep postgres_available
   curl -s http://localhost:8007/metrics | grep sink_failures
   ```

5. Check late-event and DLQ rates:
   ```bash
   curl -s http://localhost:8007/metrics | grep -E "(events_late|dlq_total|events_failed)"
   ```

6. If the streaming job is stuck, restart it:
   ```bash
   docker compose --profile streaming restart spark-streaming
   ```
   The job resumes from its last checkpoint — no data loss, but a brief
   gap in processing-lag metrics is expected.

## Redis Outage Triage

If `RedisUnavailable` fires:

1. Check Redis container: `docker compose ps redis`
2. Check Redis CLI: `docker compose exec redis redis-cli ping`
3. The platform **continues serving** during a Redis outage — analytics-
   service falls back to PostgreSQL for cache misses and fails open on
   rate limiting. This is by design (see the `redis-outage` reliability
   exercise).
4. Monitor fallback rate:
   ```bash
   curl -s http://localhost:8003/metrics | grep platform_cache_available
   curl -s http://localhost:8003/metrics | grep 'outcome="unavailable"'
   ```
5. Once Redis is restored, the cache warms naturally on the next request
   cycle. No manual cache warm-up is required.

## DLQ Replay

Use dry-run first:

```bash
PYTHONPATH=.:services/shared python -m platform_cli --pretty replay dlq --event-id evt_123 --dry-run
```

Replay should only happen after validating the contract, checking idempotency, and confirming downstream storage is healthy.

## Backfill

Use bounded date windows and tenant-specific runs:

```bash
PYTHONPATH=.:services/shared python -m platform_cli --pretty backfill metrics \
  --tenant-id tenant_demo \
  --start-date 2026-06-01 \
  --end-date 2026-06-03 \
  --dry-run
```

After execution, run reconciliation and clear urgent Redis keys if affected metrics were cached.

## Optional Airflow Runs

Airflow can schedule the same finite operational workflows locally:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up --build
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-webserver airflow dags list
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-webserver airflow dags test cloudscale_operational_checks 2026-06-01
```

Use Airflow for validation, reconciliation dry-runs, metric backfill dry-runs, evidence generation, and finite Spark batch jobs. Keep Kafka consumers and streaming services managed by their own service processes.

## Disaster Recovery Assumptions

| Component | Local Behavior | Production Assumption |
| --- | --- | --- |
| PostgreSQL | Single local instance | PITR backups, restore validation, migrations, smoke queries. |
| Kafka | Single broker in Compose | Retained offsets, DLQ review, replay from topic retention window. |
| Redis | Disposable cache | Restart, warm cache, watch API p95 and PostgreSQL QPS. |
| Spark jobs | Scripted jobs | Re-run bounded partitions through an orchestrator. |
| Metadata | Local tables | Tenant config restore is high priority. |

## Related Reference Docs

- `docs/disaster-recovery-runbook.md`
- `docs/dlq-replay-runbook.md`
- `docs/backfill-reprocessing.md`
- `docs/reconciliation.md`
- `docs/postmortem-template.md`
- `docs/incident-simulation.md`
