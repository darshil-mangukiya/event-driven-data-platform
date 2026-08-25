# Multi-Tenant Architecture

The platform uses shared infrastructure with tenant-aware data access.

## Implemented Isolation

- `tenant_id` is required on every event envelope.
- Core tables include `tenant_id` and are indexed for tenant-scoped queries.
- APIs check caller tenant headers against requested tenant IDs.
- Analytics endpoints only query tenant-scoped rows.
- Tenant config is centralized in `tenant_config`.
- RBAC mapping is scaffolded in `tenant_users`.
- Optional PostgreSQL RLS policies are provided in `database/security/tenant_rls.sql`.
- `scripts/validate_tenant_rls.py` checks policy coverage for tenant tables.

## Tradeoffs

Shared schema advantages:

- Easier local development and operational simplicity.
- Efficient cross-tenant platform monitoring.
- Reusable service code and aggregate logic.
- Lower cost for a fast-growing company before heavy enterprise isolation needs.

Shared schema risks:

- Every query must enforce tenant filters.
- Noisy tenants can affect shared database resources.
- Stronger compliance requirements may need separate databases or clusters.

Production evolution path:

1. Shared schema with strict tenant filters and tests.
2. Shared database with partitioning and row-level security.
3. Dedicated schemas or databases for enterprise tenants.
4. Dedicated infrastructure for regulated or very large tenants.

See `docs/tenant-isolation-validation.md` for the RLS validation workflow.
