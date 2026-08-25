# Analytics Query Plans

These SQL files are the expected `EXPLAIN` probes for high-traffic analytics endpoints.

Run against a local Postgres instance:

```bash
psql "$DATABASE_URL" -f sql/explain/analytics_revenue.sql
psql "$DATABASE_URL" -f sql/explain/analytics_product_performance.sql
psql "$DATABASE_URL" -f sql/explain/analytics_marketing_roi.sql
psql "$DATABASE_URL" -f sql/explain/system_status.sql
```

Expected access patterns:

- `tenant_metrics_daily` should use the primary key or tenant/date ordering for revenue, churn, retention, and customer metrics.
- `processed_orders` should use tenant/date and tenant/campaign indexes for product performance and marketing ROI.
- `tenant_products` joins should use `(tenant_id, product_id)`.
- `service_health_metrics` should use tenant/time index; a production optimization could add `(tenant_id, service_name, event_timestamp desc)` for the `distinct on` pattern.

Local `EXPLAIN` files are intentionally checked in as query probes, not as claimed plan outputs. Actual plans depend on row counts, table statistics, and Postgres version.

