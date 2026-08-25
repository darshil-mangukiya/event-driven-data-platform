# Production Hardening

For deployment-specific hardening, see `docs/deployment-hardening.md`. This page focuses on platform behavior and operational readiness.

This project includes deployment-hardening guidance beyond the local runtime.

## Authentication

- JWT token issuing is available through `metadata-service` at `POST /auth/token`.
- Services accept `Authorization: Bearer <token>` and fall back to local demo headers.
- Tokens include `tenant_id`, `role`, and `scopes`.

## API Usage Logging

The analytics service writes request metadata to `api_usage_log`, including tenant, endpoint, status code, latency, cache status, and timestamp. This supports chargeback, audit, adoption tracking, and noisy-tenant analysis.

## Observability

Services expose Prometheus-format `/metrics` endpoints with:

- Request counters.
- Request latency histograms.
- Kafka publish/process counters.
- Redis cache operation counters.

## Migration Path

Alembic scaffolding is included for production deployments. Docker init SQL remains the fast local bootstrap path.

## Tenant Security

`database/security/tenant_rls.sql` provides row-level security policies applied
during fresh local initialization. Tenant-scoped runtime paths set
`app.tenant_id` transaction-locally before protected queries.

## Lakehouse Path

MinIO simulates S3-compatible object storage for bronze/silver/gold parquet layouts. PostgreSQL remains the low-latency serving store; object storage is used for replay, backfill, and larger analytical jobs.

## Operational Evidence

The platform includes DLQ replay audit logs, data quality checks, benchmark result capture, and a small dashboard service so reviewers can see how the system is operated after deployment.
