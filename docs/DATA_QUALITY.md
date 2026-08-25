# Data Quality

The data quality runner checks tenant-level freshness, validity, uniqueness, range constraints, reference integrity, and volume anomalies.

## Run

```bash
PYTHONPATH=services/shared python scripts/run_data_quality_checks.py --pretty
```

Run one tenant without writing results:

```bash
PYTHONPATH=services/shared python scripts/run_data_quality_checks.py \
  --tenant-id tenant_demo \
  --dry-run \
  --pretty
```

## Result Tables

- `data_quality_check_results`
- `data_quality_score_daily`

## Implemented Checks

- `raw_event_freshness`
- `raw_event_required_fields`
- `processed_order_event_uniqueness`
- `processed_order_revenue_ranges`
- `processed_payment_status_domain`
- `processed_order_product_reference`
- `tenant_metric_non_negative_ranges`
- `raw_event_volume_anomaly`

The quality score starts at 100 and applies penalties for warnings, failures, and critical failures.

## Related Workflows

- Reconciliation: `scripts/reconcile_metrics.py`
- Reconciliation summary: `scripts/reconciliation_summary.py`
- Metric contracts: `metrics/contracts/tenant_daily_metrics.json`
- Semantic catalog: `docs/semantic-catalog.md`
- KPI lineage: `docs/kpi-lineage.md`
