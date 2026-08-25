# Security and Tenant Isolation

The platform combines application authorization, PostgreSQL row-level security,
and per-principal rate limiting. The local deployment includes explicit
development conveniences; this document records their scope.

## Authentication Modes

`AUTH_MODE=strict` is the code default. Requests require a signed
`Authorization: Bearer <jwt>` token, and unsigned identity headers are ignored.
Local HS256 tokens validate signature, issuer, audience, and expiration.

OIDC tokens use JWKS key selection with RS256 signature verification, issuer
validation, expiration validation, and optional audience validation. Configure
`OIDC_JWKS_URL`, `OIDC_ISSUER`, and, where applicable, `OIDC_AUDIENCE`.
OIDC principals must include the configured tenant claim (`tenant_id` by
default) as a non-empty string. Missing, empty, null, and non-string tenant
claims are rejected.

`AUTH_MODE=dev_compat` enables the header-based local demo workflow. When a
request has no bearer token, the service trusts `X-Tenant-Id`, `X-User-Id`, and
`X-User-Role`. Any caller that can reach such a service can choose those values,
including the administrative role. Restrict this mode to an isolated local
environment.

The checked-in `.env.example` selects `dev_compat` for Docker Compose examples.
Deployments must set `AUTH_MODE=strict` and replace all placeholder secrets.
`AUTH_MODE=header` remains a deprecated compatibility alias for `dev_compat`.

Validate configuration with:

```bash
python scripts/validate_auth_posture.py --pretty
python scripts/validate_auth_posture.py --require-strict --pretty
```

## Database Roles

| Role | Purpose | Superuser | BYPASSRLS |
| --- | --- | :---: | :---: |
| `platform` | Database initialization and administration | Yes | Yes |
| `platform_tenant_scoped` | Tenant-facing runtime queries and writes | No | No |
| `platform_admin_bypass` | Cross-tenant local operations and metadata paths | No | Yes |

Tenant-facing services use `platform_tenant_scoped`:

- analytics service;
- processing service;
- demo dashboard.

Cross-tenant local services use `platform_admin_bypass`:

- metadata service;
- schema registry service;
- ops console;
- optional Spark JDBC sink.

The `platform` superuser is reserved for PostgreSQL initialization and
administrative commands.

## Row-Level Security

Eleven tenant tables enable and force RLS. Policies compare each row's
`tenant_id` with `current_app_tenant_id()` from
`database/security/tenant_rls.sql`.

Tenant-scoped repository methods open a transaction and run:

```sql
select set_config('app.tenant_id', $1, true);
```

The final argument makes the value transaction-local. It expires before the
connection returns to the asyncpg pool, preventing tenant context from carrying
into the next request. `WITH CHECK` policies also reject cross-tenant writes.

Fresh PostgreSQL volumes apply the RLS script from
`database/init/004_apply_tenant_rls.sql`. Volumes created before that
initialization file was introduced require a one-time manual application:

```bash
make rls-apply
make rls-check-live
```

Static policy validation is available through `make rls-check`.

## Service Exposure

Host-published local services bind to loopback by default in Docker Compose.

The ingestion, analytics, and metadata APIs use the shared authentication
dependency. Health and metrics endpoints are intended for local orchestration
and monitoring.

Demo dashboard authentication is not configured. Docker Compose publishes the
interface on loopback by default. Users
can select an available tenant, while protected-table reads execute under
`platform_tenant_scoped` with transaction-local tenant context and enforced
RLS. The raw Kubernetes manifests and Helm chart do not package this service.

Ops console authentication is not configured. Docker Compose publishes the
interface on loopback by default. It uses
`platform_admin_bypass` for cross-tenant backlog, pipeline, and service-health
views. Add administrative authentication and network restrictions before any
shared deployment.

Schema registry authentication is not configured. Docker Compose publishes the
service on loopback by default; add authentication and network restrictions
before any shared deployment.

## Rate Limiting

Redis rate limits use `tenant_id:user_id:path` keys. Their trust boundary is the
resolved principal: strict mode uses validated token claims; `dev_compat` uses
caller-supplied headers.

## Secrets

- `.env` files are ignored.
- `.env.example`, Compose, and local manifests contain recognizable development
  placeholders.
- Helm values marked as secrets are local defaults and should be supplied from
  a deployment secret manager.
- The repository contains no external provider key.

Run `scripts/validate_auth_posture.py` as a deployment gate and rotate all
environment credentials outside local development.

## Verification

Relevant checks include:

- `tests/test_auth_hardening.py`;
- `tests/test_oidc.py`;
- `tests/test_rls_runtime.py`;
- `tests/test_database_tenant_scoping.py`;
- `tests/test_tenant_access_and_cache.py`;
- `scripts/validate_auth_posture.py`;
- `scripts/validate_tenant_rls.py`.

Recorded results are indexed in [../evidence/README.md](../evidence/README.md).
