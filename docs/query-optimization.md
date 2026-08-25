# Query Optimization Evidence

The analytics API is designed around tenant-scoped serving queries. The hot path is `(tenant_id, time)` and the schema indexes reflect that.

## Probe Files

Query probes live in `sql/explain/`:

- `analytics_revenue.sql`
- `analytics_product_performance.sql`
- `analytics_marketing_roi.sql`
- `system_status.sql`

## Index Strategy

| Query family | Table | Index used or expected |
| --- | --- | --- |
| Revenue/customers/churn/retention | `tenant_metrics_daily` | Primary key `(tenant_id, metric_date)` |
| Product performance | `processed_orders` | `idx_processed_orders_product`, `idx_processed_orders_tenant_date` |
| Marketing ROI | `processed_orders` | `idx_processed_orders_campaign` |
| Alerts | `alerts` | `idx_alerts_tenant_status` |
| System status | `service_health_metrics` | `idx_service_health_tenant_time` |

## Slow Query Investigation

1. Confirm the request is tenant-scoped.
2. Run the matching `EXPLAIN` probe.
3. Check whether the query is scanning outside the tenant/date slice.
4. Check table statistics and vacuum/analyze freshness.
5. Consider serving-table pre-aggregation before adding broad indexes.
6. Add indexes only when they match a stable access pattern.

## Production Improvement Candidates

- Add `(tenant_id, service_name, event_timestamp desc)` for service health status.
- Range partition raw and processed tables by event month.
- Cluster or BRIN-index very large append-only tables.
- Move long historical API ranges to precomputed monthly aggregates.

