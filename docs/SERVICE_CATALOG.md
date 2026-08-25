# Service Catalog

## Ingestion Service

- Owner: Data Platform
- Path: `services/ingestion-service`
- Port: `8001`
- Purpose: Validate tenant-scoped events and publish event envelopes to Kafka.
- Key endpoints: `/events`, `/events/batch`, `/generate/demo`, `/health`, `/metrics`, `/system/status`
- Dependencies: Kafka, shared schemas, auth helper.
- Kafka topics: `platform.events.orders`, `platform.events.payments`, `platform.events.users`, `platform.events.products`, `platform.events.system`
- Direct tables: none in the main local flow.
- Failure modes: invalid payload, unauthorized tenant access, Kafka unavailable, producer timeout.

## Processing Service

- Owner: Data Platform
- Path: `services/processing-service`
- Port: `8002`
- Purpose: Consume Kafka events, write raw/processed records, update aggregates, and track processing status.
- Key endpoints: `/health`, `/metrics`, `/system/status`
- Dependencies: Kafka, PostgreSQL, shared schemas.
- Kafka topics: all domain topics, retry topic, DLQ topic.
- Tables: `event_inbox`, `raw_events`, `processed_orders`, `processed_payments`, `processed_user_sessions`, `tenant_products`, `tenant_metrics_hourly`, `tenant_metrics_daily`, `alerts`, `pipeline_watermarks`
- Failure modes: consumer lag, database write failure, contract violation, duplicate event, DLQ growth.

## Analytics Service

- Owner: Data Platform / Analytics Platform
- Path: `services/analytics-service`
- Port: `8003`
- Purpose: Serve governed tenant-scoped metrics through REST APIs.
- Key endpoints: `/metrics/revenue`, `/metrics/customers`, `/metrics/churn`, `/metrics/retention`, `/metrics/marketing_roi`, `/metrics/product_performance`, `/metrics/payment_success`, `/metrics/event_throughput`, `/metrics/tenant_health_score`, `/alerts`, `/system/status`, `/health`, `/metrics`
- Dependencies: PostgreSQL, Redis, auth helper.
- Tables: `tenant_metrics_daily`, `tenant_metrics_hourly`, `processed_orders`, `tenant_products`, `alerts`, `service_health_metrics`, `api_usage_log`
- Failure modes: Redis unavailable, slow PostgreSQL query, stale metrics, cross-tenant request rejection, rate limit exceeded.

## Metadata Service

- Owner: Data Platform
- Path: `services/metadata-service`
- Port: `8004`
- Purpose: Manage tenants, users, product metadata, RBAC scaffold, and local token issuing.
- Key endpoints: `/auth/token`, `/tenants`, `/tenants/{tenant_id}/users`, `/tenants/{tenant_id}/products`, `/health`, `/metrics`, `/system/status`
- Dependencies: PostgreSQL, auth helper.
- Tables: `tenant_config`, `tenant_users`, `tenant_products`
- Failure modes: unauthorized admin action, invalid role, database unavailable.

## Ops Console

- Owner: Platform Operations
- Path: `services/ops-console`
- Port: `8006`
- Purpose: Show operational health, alerts, readiness status, and reliability signals.
- Dependencies: Analytics service, PostgreSQL, monitoring tables.
- Tables: `alerts`, `service_health_metrics`, `pipeline_run_log`, `api_usage_log`, `data_quality_score_daily`, `reconciliation_audit`
- Failure modes: stale platform status, missing health data, database unavailable.

## Demo Dashboard

- Owner: Data Platform Demo
- Path: `services/demo-dashboard`
- Port: `8005`
- Purpose: Provide a lightweight tenant dashboard for local validation.
- Dependencies: Analytics service.
- Failure modes: analytics service unavailable, tenant has no seeded metrics.

## Airflow Local Orchestration

- Owner: Data Platform
- Path: `airflow/`
- Port: `8088` for the optional local webserver overlay.
- Purpose: Schedule finite operational checks and batch workflows that already exist as scripts or CLI commands.
- DAGs: `cloudscale_operational_checks`, `cloudscale_batch_jobs`
- Dependencies: local Airflow metadata database, repository scripts, PostgreSQL, Spark runtime for Spark tasks.
- Tables touched by downstream tasks: `tenant_metrics_daily`, `pipeline_run_log`, `reconciliation_audit`, validation and evidence files when selected.
- Not responsible for: Kafka streaming consumers or service runtime supervision.
- Failure modes: missing Docker runtime, failed validation script, unavailable PostgreSQL, missing Spark runtime/JDBC dependency, stale local credentials.
