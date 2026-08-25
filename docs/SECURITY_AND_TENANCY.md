# Security And Tenancy

The platform uses tenant-aware application checks locally and includes database hardening scripts for a stricter production path.

## Auth Flow

Local services support two auth modes:

- Demo headers: `X-Tenant-ID`, `X-User-ID`, `X-User-Role`
- JWT bearer tokens issued by `metadata-service` at `POST /auth/token`

JWTs include:

- `sub`
- `tenant_id`
- `role`
- `scopes`
- `iss`
- `aud`
- `iat`
- `exp`

## Roles

- `platform_admin`: administrative access across tenants in local tooling.
- `tenant_admin`: administrative access within one tenant.
- `analyst`: tenant-scoped metric read access.
- `viewer`: tenant-scoped read-only access.
- `service_account`: tenant-scoped automation access.
- `tenant_analyst`: backwards-compatible local alias for `analyst`.

Unknown roles are rejected by the shared auth helper.

## Tenant Authorization

Tenant-scoped APIs validate that the requested `tenant_id` matches the authenticated principal. Platform admins are the only role designed for cross-tenant operations. Service accounts remain scoped unless a production identity system issues broader credentials.

## Audit Logging

The analytics service writes request audit records to `api_usage_log`, including:

- `tenant_id`
- `user_id`
- `endpoint`
- `status_code`
- `latency_ms`
- `cache_status`
- `role`
- `trace_id`
- `requested_at`

## PostgreSQL RLS Path

`database/security/tenant_rls.sql` defines row-level security policies using a
transaction-local setting pattern:

```sql
set app.current_tenant = 'tenant_demo';
```

Fresh local databases apply the policies automatically. Tenant-scoped runtime
paths connect through `platform_tenant_scoped` and set tenant context within
each protected transaction; cross-tenant tools use the separate
`platform_admin_bypass` role.

## Validation

```bash
python scripts/validate_tenant_rls.py
PYTHONPATH=.:services/shared python -m pytest tests/test_tenant_access_and_cache.py -q
```

## Production Hardening

- Use a real identity provider for users and service accounts.
- Store JWT signing keys in a secrets manager.
- Rotate service account credentials.
- Enforce scopes in every route.
- Preserve the scoped/bypass database-role split in deployed environments.
- Send audit logs to centralized log storage.
- Add trace propagation through API, Kafka, processing, and analytics serving.
