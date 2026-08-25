# Kafka-Consumer ↔ dbt Metric Reconciliation

Scope: Local verification
Date: 2026-08-22

## Status: LOCALLY VERIFIED (live run, real data)

## An important architectural correction

A earlier review's finding was: "the Kafka-consumer and dbt paths
independently derive overlapping metrics" with "no automated direct
reconciliation." Precise inspection of
`dbt/models/marts/fct_tenant_daily_metrics.sql` during verification found
that framing was **partially imprecise**, and verification corrects it
rather than building a reconciliation around a false premise:

- `net_revenue`, `gross_revenue`, `order_count`, `units_sold`,
  `new_users`, `active_users`, and `churn_signal_count` are read
  **directly, unchanged** (`select * from {{ source('platform',
  'tenant_metrics_daily') }}`) by dbt — there is only **one** code path
  computing these today (`services/processing-service/app/repository.py`'s
  incremental upserts). dbt re-exposes them through a governed mart; it
  does not independently recompute them. Reconciling a passthrough value
  against its own source would always trivially match by construction and
  would never detect a runtime defect — so it is documented as
  `NOT_APPLICABLE`, not fabricated as a meaningful check.
- `payment_success_count` and `payment_failure_count` are the **one**
  genuine case of two independent derivations: the Kafka-consumer path
  incrementally upserts them event-by-event, while
  `fct_tenant_daily_metrics.sql` **already computed** a fresh,
  independent `observed_successful_payments`/`observed_failed_payments`
  pair via a direct `COUNT(*) ... GROUP BY` over `processed_payments`
  (through `stg_payments`) — sitting right next to the serving path's own
  numbers in the same row, but **nothing ever compared them**. This is
  the real gap, and it's narrower and more precise than the original
  framing suggested.

See [`metrics/contracts/kafka_dbt_metric_reconciliation.json`](../../metrics/contracts/kafka_dbt_metric_reconciliation.json)
for the full, machine-readable version of this distinction.

## What was built

- **`metrics/contracts/kafka_dbt_metric_reconciliation.json`** — the
  contract: for each of the 2 reconciled metrics, the serving
  source, the dbt source, grain, tenant key, both formulas, allowed
  tolerance (0, exact match expected — both sides count the same
  underlying events), criticality, and a quality-gate rule. Also
  explicitly lists the 5 passthrough fields as `NOT_APPLICABLE`, with the
  reasoning, so their absence from reconciliation isn't mistaken for an
  oversight.
- **`scripts/reconcile_kafka_dbt_metrics.py`** — loads the contract,
  queries both `tenant_metrics_daily` (serving) and
  `analytics_mart.fct_tenant_daily_metrics` (dbt) from the same
  PostgreSQL instance, joins by `(tenant_id, metric_date)`, and reports
  one row per `(tenant, date, metric)` with `serving_value`, `dbt_value`,
  `absolute_difference`, `relative_difference`, `allowed_tolerance`, and
  `status` (`MATCH` / `WITHIN_TOLERANCE` / `FAIL` / `MISSING_IN_SERVING` /
  `MISSING_IN_DBT`). Exits non-zero if any row is `FAIL`. **Never writes
  to either table** (no `UPDATE`/`INSERT` path exists at all — verified
  by a dedicated test asserting the mock connection's `.execute()` is
  never called).
- **`tests/test_kafka_dbt_metric_reconciliation.py`** — 12 tests,
  offline/deterministic: exact match, within tolerance, outside tolerance
  (FAIL), zero-denominator (no division error), missing-in-serving,
  missing-in-dbt, multi-tenant isolation (a wrong value for tenant B must
  not affect tenant A's result), the no-rewrite guarantee, and a contract
  self-consistency check.

## Live run (real data, verification)

```
$ PYTHONPATH=services/shared .venv/bin/python \
    scripts/reconcile_kafka_dbt_metrics.py \
    --database-url postgresql://platform:[REDACTED]@127.0.0.1:15432/data_platform --pretty

{
  "overall_status": "PASS",
  "rows_checked": 12,
  "tenant_date_pairs_checked": 6,
  "critical_failures": 0,
  "missing_rows": 0,
  ...
}
```

All 12 rows (2 metrics × 3 tenants × 2 dates in the seed dataset) report
`status: "MATCH"` and `absolute_difference: 0`. The data came from the
`dbt build` recorded in `dbt-live-verification.md`.

## Test run

```
$ PYTHONPATH=.:services/shared .venv/bin/python -m pytest -q \
    tests/test_kafka_dbt_metric_reconciliation.py
12 passed in 0.38s
```

## Quality gate

`test_run_reconciliation_flags_a_real_mismatch_as_fail` supplies a
dbt-side value that disagrees with the serving-side value and confirms the
script reports `overall_status: "FAIL"` and the row as `status: "FAIL"`.

## Limitations

- Only two metrics currently have a second derivation to reconcile.
- Checked against a single 3-tenant, 2-day demo dataset — not a
  large-scale or long-running reconciliation history.
- No scheduled/automated recurring run exists yet (this is a CLI script,
  invoked manually or from CI, not a cron job or Airflow DAG).
- If a future dbt model independently recomputes `net_revenue` or
  `order_count` from `processed_orders` (rather than passthrough), this
  contract and script should be extended — deliberately not done this
  pass, since no such independent computation exists to reconcile against
  today (per "Do not compare differently-defined metrics as though they
  are identical").
