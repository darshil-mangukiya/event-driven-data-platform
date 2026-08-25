# PostgreSQL RLS Runtime Verification

Status: **VERIFIED**

Date: 2026-08-22

## Scope

This record covers PostgreSQL row-level security under the dedicated
runtime roles defined in `database/security/tenant_rls.sql`.

## Enforced posture

- Eleven tenant-owned tables enable and force RLS.
- `platform_tenant_scoped` is `NOSUPERUSER NOBYPASSRLS`.
- `platform_admin_bypass` is `NOSUPERUSER BYPASSRLS`.
- Policies apply both `USING` and `WITH CHECK` expressions based on
  `current_app_tenant_id()`.
- Missing tenant context returns no tenant-owned rows.

## Runtime matrix

The following command was executed against local PostgreSQL:

```bash
python scripts/validate_tenant_rls.py --live --pretty
```

| Check | Result |
| --- | --- |
| Tenant A sees its rows | PASS |
| Tenant A cannot see Tenant B rows | PASS |
| Tenant B cannot see Tenant A rows | PASS |
| Missing tenant context fails closed | PASS |
| Cross-tenant insert is rejected | PASS |
| Administrative bypass role sees multiple tenants | PASS |

The cross-tenant insert returned PostgreSQL's row-level security policy
violation. The static validator also requires `FORCE ROW LEVEL SECURITY`
for every protected table.

## Service integration

Tenant-facing analytics reads and processing-service writes use
`platform_tenant_scoped`. Administrative and platform-wide local tools use
`platform_admin_bypass`. Bootstrap DDL and role creation use the
`platform` superuser; application runtime services do not.

The application connection mapping and pool-safety checks are recorded in
`application-rls-runtime-verification.md`.
