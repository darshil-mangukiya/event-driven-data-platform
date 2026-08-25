# Disaster Recovery Runbook

This runbook describes recovery behavior for the local platform. Production values must be approved by business stakeholders and tested with restore drills.

## Recovery Targets

| Component | RPO target | RTO target | Notes |
| --- | ---: | ---: | --- |
| PostgreSQL serving layer | 15 minutes | 60 minutes | Use PITR backups and restore validation. |
| Kafka event backbone | Topic retention window | 30 minutes | Replay from retained offsets or DLQ. |
| Redis cache | 0 minutes | 10 minutes | Cache is disposable; APIs fall back to Postgres. |
| Spark batch jobs | Last successful partition | 4 hours | Re-run bounded partitions. |
| Metadata service | 15 minutes | 60 minutes | Tenant config restore is high priority. |

## PostgreSQL Backup & Restore

Previously, this section described a "Restore latest valid snapshot
plus WAL/PITR" procedure with **no snapshot-taking mechanism anywhere in
the repo** — production-style continuous WAL archiving/point-in-time
recovery, which this local docker-compose project never actually sets up.
What follows is what a local/single-instance setup can actually do today,
backed by real, working, live-verified tooling:

```bash
make backup-postgres          # pg_dump custom-format backup + row-count manifest
make backup-restore-drill     # full drill: backup -> restore into a scratch DB -> verify row counts -> drop scratch DB
make restore-postgres-dry-run DUMP=backups/data_platform-<timestamp>.dump   # validate a backup without touching any database
```

`scripts/backup_postgres.py` and `scripts/restore_postgres.py` run
`pg_dump`/`pg_restore` **inside the running postgres container** when one
is available (`docker exec`), rather than depending on the host's installed
client version — found live during verification: the local environment's host
`pg_dump` (14.19) flatly refused to dump a PostgreSQL 16 server
(`error: aborting because of server version mismatch`). Falls back to a
host `pg_dump`/`pg_restore` binary only when no docker-compose postgres
container is running.

1. Freeze writes or route services to maintenance mode.
2. Restore the most recent `.dump` file with `scripts/restore_postgres.py
   --dump <file> --target-database-url <url>` (verification is on by
   default — it re-counts every backed-up table and reports `status:
   failed` on any mismatch, in addition to a non-zero `pg_restore` exit code,
   since `pg_restore` commonly exits non-zero on harmless ownership
   warnings even on an otherwise-successful restore).
3. Apply migrations (`make migrate`) if the backup predates a schema change.
4. Run smoke queries against tenant tables.
5. Run `scripts/reconcile_metrics.py` for impacted tenants.
6. Re-enable analytics APIs and monitor latency/errors.

Production PITR/continuous WAL archiving remains out of scope for this
local project — see `docs/LIMITATIONS.md` "Disaster Recovery Scope".

## Kafka Recovery

1. Confirm broker/topic health and consumer group offsets.
2. Pause processing workers if downstream storage is unhealthy.
3. Restart processing from last committed offsets after Postgres is stable.
4. Replay DLQ events only after idempotency review.
5. Watch consumer lag, DLQ growth, and duplicate raw-event skips.

## Redis Recovery

Redis loss should not lose source-of-truth data. Restart Redis, allow cache warmup, and monitor API p95 latency and Postgres QPS.

## Backfill Recovery

Use backfills when serving aggregates are stale but processed facts are correct:

```bash
PYTHONPATH=services/shared python scripts/backfill_metrics.py \
  --tenant-id tenant_demo \
  --start-date 2026-05-01 \
  --end-date 2026-05-07 \
  --dry-run \
  --pretty
```

Then execute, reconcile, and clear urgent cache keys.

## Drill Cadence

| Drill | Frequency | Evidence |
| --- | --- | --- |
| Postgres restore rehearsal | Quarterly | `make backup-restore-drill` output (restore timestamp and per-table row-count verification), smoke results, and a reconciliation report against the restored data. |
| DLQ replay rehearsal | Monthly | Replay audit rows and duplicate-skip count. |
| Redis cold-cache test | Monthly | API p95 before/after warmup. |
| Backfill rehearsal | Monthly | Backfill plan, success rows, reconciliation pass. |
