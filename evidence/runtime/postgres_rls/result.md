# PostgreSQL RLS Runtime Result

Status: **EXECUTED AND VERIFIED** on PostgreSQL 16.

All 11 tenant-bearing tables have row-level security enabled and forced. Tenant
A and Tenant B each read only their rows, cross-tenant reads returned zero,
cross-tenant inserts were rejected, and a missing tenant context failed closed.
The explicit `platform_admin_bypass` role could read multiple tenants as
designed.

The application sets `app.tenant_id` transaction-locally before tenant-scoped
work. Existing tests repeatedly reuse the pool across alternating tenant
requests and validate zero forbidden rows, covering context reset/leakage. This
local test does not represent external penetration testing.
