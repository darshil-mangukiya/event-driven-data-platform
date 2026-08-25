# Application RLS Runtime Verification

Status: **VERIFIED**

Date: 2026-08-23

## Runtime role mapping

| Component | Database role | Scope |
| --- | --- | --- |
| analytics-service | `platform_tenant_scoped` | Tenant-scoped reads |
| processing-service | `platform_tenant_scoped` | Tenant-scoped writes |
| metadata-service | `platform_admin_bypass` | Platform-wide metadata |
| ops-console | `platform_admin_bypass` | Local cross-tenant operations |
| demo-dashboard (Compose) | `platform_tenant_scoped` | Tenant-scoped local demonstration reads |
| schema-registry-service | `platform_admin_bypass` | Platform-wide schemas |
| spark-streaming | `platform_admin_bypass` | Cross-tenant batch sink |
| postgres bootstrap | `platform` | Initialization and migrations |
| ingestion-service | none | Kafka only |

The two runtime roles are non-superusers. Only the explicitly privileged
`platform_admin_bypass` role has `BYPASSRLS`.

The raw Kubernetes manifests and Helm chart package the four core services,
not the demo dashboard. They therefore define no dashboard database role.

## Tenant context and pool safety

`Postgres.fetch_scoped()`, `fetchrow_scoped()`, and
`execute_scoped()` acquire one connection, open a transaction, and call:

```sql
select set_config('app.tenant_id', $1, true)
```

The third argument makes the setting transaction-local. The query executes on
the same connection and transaction, and the setting is discarded before that
connection returns to the pool. Regression tests reuse a one-connection pool
across sequential tenant requests and confirm that context does not leak.

## Verification results

Validation used an isolated local PostgreSQL database initialized through the
normal Compose SQL sequence.

| Check | Result |
| --- | --- |
| Analytics runtime role is non-superuser/non-bypass | PASS |
| Signed tenant JWT can read its tenant | PASS |
| Signed tenant JWT is denied another tenant | PASS |
| Identical SQL returns rows only when session tenant matches | PASS |
| No tenant context returns zero protected rows | PASS |
| Processing write with matching tenant context succeeds | PASS |
| Processing write with mismatched context is rejected | PASS |
| Eleven protected tables have RLS enabled and forced | PASS |
| Compose role mappings validate | PASS |
| Kubernetes manifests parse with expected mappings | PASS |
| Helm chart lints and renders with expected mappings | PASS |

The processing repository was invoked directly against PostgreSQL because the
local Kafka container was unavailable during that recorded run. The same
repository method, SQL, and database role used by the service were exercised;
container startup was not part of that check.

## References

- Policy and roles: `database/security/tenant_rls.sql`
- Fresh initialization: `database/init/004_apply_tenant_rls.sql`
- Database helper: `services/shared/platform_shared/database.py`
- Runtime tests: `tests/test_database_tenant_scoping.py`,
  `tests/test_rls_runtime.py`
- Static/runtime validator: `scripts/validate_tenant_rls.py`
