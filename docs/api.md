# API Documentation

Each FastAPI service exposes `/docs` locally.

Exported OpenAPI contracts and fixtures live under `api/openapi/` and `api/fixtures/`. Refresh them with:

```bash
PYTHONPATH=services/shared:services/processing-service python scripts/export_openapi_contracts.py
```

## Ingestion Service

- `POST /events`: validate and publish a single event.
- `POST /events/batch`: publish up to 500 events.
- `POST /generate/demo`: generate realistic local sample events for a tenant.
- `GET /health`
- `GET /system/status`

Business producers should send `idempotency_key` or immutable `event_id` for replay-safe ingestion.

## Analytics Service

All metric endpoints require a `tenant_id` query parameter and `X-Tenant-ID` header unless the caller uses the `platform_admin` role.

- `GET /metrics/revenue`
- `GET /metrics/customers`
- `GET /metrics/churn`
- `GET /metrics/retention`
- `GET /metrics/marketing_roi`
- `GET /metrics/product_performance`
- `GET /metrics/payment_success`
- `GET /metrics/event_throughput`
- `GET /metrics/tenant_health_score`
- `GET /alerts`
- `GET /tenants`
- `GET /system/status`
- `GET /health`

Common query parameters:

- `tenant_id`
- `start_date`
- `end_date`
- `limit`
- `offset`

## Metadata Service

- `POST /auth/token`
- `GET /tenants`
- `PUT /tenants/{tenant_id}`
- `GET /tenants/{tenant_id}/users`
- `PUT /tenants/{tenant_id}/users/{user_id}`
- `GET /tenants/{tenant_id}/products`
- `GET /health`
- `GET /system/status`

## Auth Scaffold

The local implementation uses headers:

- `X-Tenant-ID`
- `X-User-ID`
- `X-User-Role`

Production should replace this with signed JWTs, service identity, and policy enforcement at the gateway and service layers.

The service layer also supports JWT bearer tokens for local demos. Issue a token through the metadata service and pass it to other services:

```text
Authorization: Bearer <token>
```
