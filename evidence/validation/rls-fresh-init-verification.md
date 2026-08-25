# RLS Fresh-Initialization Verification

Status: **VERIFIED**

Date: 2026-08-23

## Initialization path

PostgreSQL initialization runs these files in order:

1. `001_schema.sql`
2. `002_seed.sql`
3. `003_local_demo_transactional_seed.sql`
4. `004_apply_tenant_rls.sql`

The fourth file includes the canonical policy definition from
`database/security/tenant_rls.sql`. Compose, raw Kubernetes manifests, and
the Helm chart mount the same sequence.

## Fresh-volume check

An isolated local volume was initialized through the normal container entry
point. No manual policy command was applied afterward.

| Check | Result |
| --- | --- |
| Protected tables present | 11/11 |
| RLS enabled | 11/11 |
| RLS forced | 11/11 |
| Tenant policies present | 11 |
| `platform_tenant_scoped` | non-superuser, non-bypass |
| `platform_admin_bypass` | non-superuser, bypass |
| Tenant A/B visibility matrix | PASS |
| Missing context fails closed | PASS |
| Cross-tenant insert rejection | PASS |

## Packaging consistency

`docker compose config --quiet`, Helm lint/template, and raw Kubernetes YAML
parsing confirm that the initialization files are mounted at the expected
paths. `tests/test_rls_runtime.py` and
`tests/test_database_tenant_scoping.py` cover the policy and role invariants.
