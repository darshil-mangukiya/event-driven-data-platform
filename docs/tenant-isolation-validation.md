# Tenant Isolation Validation

Tenant isolation is enforced by signed tenant claims, application
authorization, tenant-scoped SQL, and PostgreSQL row-level security.

## Application controls

- Event envelopes and tenant-facing API requests require `tenant_id`.
- Shared auth helpers reject cross-tenant reads unless the caller has the
  platform administrator role.
- Serving queries include tenant predicates.
- Tests cover cross-tenant API rejection and repository scoping.

## Database controls

`database/security/tenant_rls.sql` enables and forces RLS on tenant-owned
tables. Tenant-facing services use the non-superuser,
non-`BYPASSRLS` `platform_tenant_scoped` role.

```bash
python scripts/validate_tenant_rls.py --emit-validation-sql
```

The validator checks policy coverage, `FORCE ROW LEVEL SECURITY`,
`USING` predicates, `WITH CHECK` predicates, and runtime role attributes.
Services set transaction-local context before scoped queries:

```sql
select set_config('app.tenant_id', 'tenant_demo', true);
```

Runtime results are recorded in
`../evidence/validation/rls-runtime-verification.md` and
`../evidence/validation/application-rls-runtime-verification.md`.
