# Event-Driven Reconciliation

Reconciliation recomputes serving metrics from processed domain tables and
stores per-run results in `reconciliation_audit`. The platform does not
include a CDC connector.

## Checks

| Check | Audit name | Serving fields | Recomputed source |
| --- | --- | --- | --- |
| Revenue | `tenant_metrics_daily_reconciliation` | net revenue, orders, units | `processed_orders` |
| Payment | `payment_reconciliation` | successful and failed payments | `processed_payments` |
| Customer activity | `customer_activity_reconciliation` | new users, active users, churn signals | `processed_user_sessions` |

Each SQL query builds a tenant/date series, recomputes source facts, and
left-joins `tenant_metrics_daily`. Pure evaluation functions calculate
deltas and status. Revenue deltas use dedicated audit columns; payment and
customer-activity deltas are stored in `details`.

## Commands

```bash
PYTHONPATH=.:services/shared python scripts/reconcile_metrics.py \
  --tenant-id tenant_demo \
  --start-date 2026-05-01 \
  --end-date 2026-05-07 \
  --check all \
  --dry-run \
  --pretty

python -m platform_cli ops reconciliation-run \
  --tenant-id tenant_demo \
  --start-date 2026-05-01 \
  --end-date 2026-05-07 \
  --check all \
  --dry-run

python -m platform_cli ops reconciliation --tenant-id tenant_demo --days 7
```

Remove `--dry-run` to persist audit rows and correlated lineage events.
Reconciliation runs are operator-triggered; no schedule is configured.

## Drift policy

| Check | Threshold |
| --- | ---: |
| Revenue | $0.01 net-revenue delta; zero order/unit delta |
| Payment | Zero success/failure-count delta |
| Customer activity | Zero new/active/churn-count delta |

The revenue tolerance covers sub-cent decimal rounding. Count metrics require
exact equality.

## Investigation

1. Inspect `reconciliation_audit.details` for the tenant/date slice.
2. Compare `raw_events` with the corresponding `processed_*` table.
3. Check duplicate handling and consumer writes.
4. Run `scripts/backfill_metrics.py` if the serving aggregate is stale.
5. Repeat the same reconciliation check and review the daily summary.
6. Invalidate the tenant metric cache if serving values changed.

## Validation

Tests cover clean matches, missing values, exact mismatches, zero
denominators, tenant isolation, persistence shape, lineage correlation, and
CLI dispatch. Local PostgreSQL evidence recorded all three audit names and
their deltas. See
`../evidence/validation/kafka-dbt-metric-reconciliation.md` for the separate
Kafka-serving-versus-dbt comparison.
