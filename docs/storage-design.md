# Storage Design

The schema separates raw, processed, serving, metadata, and observability data.

## Table Groups

- Raw: `raw_events`
- Processed: `processed_orders`, `processed_payments`, `processed_user_sessions`, `tenant_products`
- Serving: `tenant_metrics_hourly`, `tenant_metrics_daily`, `tenant_analytics_isolated`
- Risk and alerts: `fraud_or_risk_events`, `alerts`
- Monitoring: `service_health_metrics`, `pipeline_run_log`, `api_usage_log`, `reconciliation_audit`
- Metadata: `tenant_config`, `tenant_users`

## Indexing Strategy

Tenant and time are the most important access dimensions, so indexes use `(tenant_id, event_timestamp desc)` or `(tenant_id, metric_date)`. Product and campaign performance use additional compound indexes.

## Partitioning Strategy

Local Postgres keeps ordinary tables for simplicity. Production should range-partition `raw_events`, `processed_orders`, `processed_payments`, and `processed_user_sessions` by event month. For very large tenants, add tenant hashing or clustering to avoid hot partitions.

## Retention

Recommended production retention:

- Raw events: 90 to 180 days in Postgres, longer in object storage.
- Processed tables: 12 to 24 months depending on analytics needs.
- Serving aggregates: indefinite or business-defined.
- API usage and health metrics: 30 to 90 days online, archive to object storage.
- DLQ: 14 to 30 days with replay audit.

## Migration and Security Hardening

Alembic migration scaffolding lives in `database/migrations`. Local Docker still uses init SQL for speed, while production-style deploys can run `alembic upgrade head`.

Row-level security policies live in `database/security/tenant_rls.sql` and are
applied during fresh local initialization. Tenant-scoped services set
`app.tenant_id` transaction-locally before protected queries.
