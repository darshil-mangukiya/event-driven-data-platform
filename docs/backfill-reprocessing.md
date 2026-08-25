# Backfill and Reprocessing Framework

Backfill is different from DLQ replay:

- Replay sends original events back through Kafka after a processing failure.
- Backfill rebuilds serving tables from already processed/source tables for a tenant and date range.
- Rebuild recomputes a broader mart or lakehouse partition from raw history.

## Daily Metrics Backfill

Dry-run:

```bash
PYTHONPATH=services/shared python scripts/backfill_metrics.py \
  --tenant-id tenant_demo \
  --start-date 2026-05-01 \
  --end-date 2026-05-07 \
  --dry-run \
  --pretty
```

Execute:

```bash
PYTHONPATH=services/shared python scripts/backfill_metrics.py \
  --tenant-id tenant_demo \
  --start-date 2026-05-01 \
  --end-date 2026-05-07 \
  --requested-by platform-operator
```

The CLI deletes and rebuilds `tenant_metrics_daily` for the requested tenant/date range and writes status rows to `pipeline_run_log`.

After execution, run reconciliation for the same tenant/date window:

```bash
PYTHONPATH=services/shared python scripts/reconcile_metrics.py \
  --tenant-id tenant_demo \
  --start-date 2026-05-01 \
  --end-date 2026-05-07 \
  --pretty
```

## Execution Model

The daily metrics backfill is intentionally scoped:

| Control | Implementation |
| --- | --- |
| Tenant isolation | `tenant_id` is required and every source query filters by it. |
| Bounded date window | `--max-window-days` defaults to 31 days. Larger ranges require `--allow-large-window`. |
| Idempotency | The target tenant/date slice is deleted, then recomputed from processed source tables. |
| Auditability | Each run writes status context to `pipeline_run_log`. |
| Cache follow-up | Redis metric keys expire by TTL; urgent corrections should flush tenant metric keys after success. |

The SQL reference lives in `sql/backfill/daily_metrics.sql`. The CLI renders the same delete/insert plan in dry-run mode so operators can review it before execution.

## Guardrails

- Backfill scope must include tenant and bounded date range.
- Prefer dry-run first.
- Do not run concurrent backfills for the same tenant/date range.
- Confirm downstream cache TTL or invalidation behavior after completion.
- Document business reason in `requested_by` or pipeline metadata.

## When To Use Each Path

| Scenario | Recommended path |
| --- | --- |
| Kafka consumer was down and events are in retry/DLQ | Use `scripts/dlq_tool.py` or retry replay first. |
| Source facts are correct but daily metrics are wrong | Run this daily metrics backfill for the tenant/date slice. |
| Raw history or business logic changed | Rebuild with Spark/dbt, then run serving-table validation. |
| Single tenant needs urgent correction | Backfill, verify `/metrics/revenue`, then flush tenant cache keys if TTL is too slow. |
