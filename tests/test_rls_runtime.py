"""RLS runtime-enforcement tests
(database/security/tenant_rls.sql, scripts/validate_tenant_rls.py --live).

Two real, layered bugs were found live during verification — see
tenant_rls.sql's own header comment and
evidence/validation/rls-runtime-verification.md for the full trace:

1. `enable row level security` alone doesn't protect a table against its
   own owner's connection (the `platform` role owns every table it
   created) — `force row level security` is also required.
2. Even with `force row level security`, the `platform` role — created as
   a PostgreSQL superuser by the official postgres:16 Docker image's
   `POSTGRES_USER` — bypasses RLS unconditionally; no policy setting can
   override that. A non-superuser role
   (`platform_tenant_scoped`) is required for RLS to mean anything.

`test_validate_rls_sql_requires_force_row_level_security` is a fast,
static regression test for bug #1's detection. The live runtime matrix
itself (both bugs' actual fix, tested against a real database) is
`@pytest.mark.integration` and skips cleanly without one.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))


def _load_validate_tenant_rls():
    import importlib.util

    spec = importlib.util.spec_from_file_location("validate_tenant_rls", PROJECT_ROOT / "scripts" / "validate_tenant_rls.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


validate_tenant_rls = _load_validate_tenant_rls()


def test_validate_rls_sql_requires_force_row_level_security() -> None:
    sql_missing_force = """
    create or replace function current_app_tenant_id() returns text language sql stable as $$ select 1 $$;
    alter table processed_orders enable row level security;
    create policy p on processed_orders using (tenant_id = current_app_tenant_id()) with check (tenant_id = current_app_tenant_id());
    """
    errors = validate_tenant_rls.validate_rls_sql(sql_missing_force)
    assert any("force row level security" in e for e in errors)


def test_real_tenant_rls_sql_passes_static_validation() -> None:
    sql = (PROJECT_ROOT / "database" / "security" / "tenant_rls.sql").read_text()
    errors = validate_tenant_rls.validate_rls_sql(sql)
    assert errors == [], errors


def test_real_tenant_rls_sql_creates_the_dedicated_non_superuser_roles() -> None:
    """The RLS script must create a
    non-superuser role for tenant-scoped connections, and a separate
    non-superuser BYPASSRLS role for the admin path — never grant
    BYPASSRLS to the same role tenant-scoped traffic uses.
    """
    sql = (PROJECT_ROOT / "database" / "security" / "tenant_rls.sql").read_text()
    assert "platform_tenant_scoped" in sql
    assert "nosuperuser nobypassrls" in sql
    assert "platform_admin_bypass" in sql
    assert "nosuperuser bypassrls" in sql


@pytest.mark.integration
def test_live_rls_metadata_matches_expected_posture_on_any_running_database() -> None:
    """Regression for the P6 RLS-startup-automation fix: this queries
    PostgreSQL's own system catalogs directly (pg_roles, pg_class,
    pg_policies), not tenant_rls.sql's text — so it fails if a future
    fresh local database ever starts *without* the RLS security layer
     applied (whether the automatic docker-entrypoint-initdb.d
    step silently regresses, or someone edits docker-compose.yml's mount
    without noticing). This is deliberately independent of
    test_live_rls_runtime_matrix below, which proves *behavior*
    (cross-tenant denial); this one proves the underlying *metadata* the
    behavior depends on, which is the stronger, more direct assertion
    Task 11 of the RLS-automation fix calls for.
    """
    import psycopg2

    from reliability.injectors.reachability import postgres_reachable

    admin_url = "postgresql://platform_admin_bypass:local-admin-bypass-change-me@localhost:15432/data_platform"
    if not postgres_reachable(admin_url):
        pytest.skip("no live PostgreSQL with the platform_admin_bypass role reachable at localhost:15432")

    conn = psycopg2.connect(admin_url)
    try:
        with conn.cursor() as cur:
            # Role posture: the tenant-scoped role must exist and must be
            # a non-superuser, non-bypass role (bug #2's fix).
            cur.execute("select rolsuper, rolbypassrls from pg_roles where rolname = 'platform_tenant_scoped'")
            row = cur.fetchone()
            assert row is not None, "platform_tenant_scoped role does not exist"
            rolsuper, rolbypassrls = row
            assert rolsuper is False, "platform_tenant_scoped must not be a superuser"
            assert rolbypassrls is False, "platform_tenant_scoped must not have BYPASSRLS"

            # Table posture: every table the platform  protects
            # must have both flags set (bug #1's fix — enable-only is not
            # enough against the table owner's own connection).
            cur.execute(
                "select relname from pg_class "
                "where relname = any(%s) and relrowsecurity and relforcerowsecurity",
                (list(validate_tenant_rls.EXPECTED_RLS_TABLES),),
            )
            protected = {r[0] for r in cur.fetchall()}
            missing = validate_tenant_rls.EXPECTED_RLS_TABLES - protected
            assert not missing, f"tables missing ENABLE+FORCE row level security: {sorted(missing)}"

            # Policy posture: every protected table must have at least
            # one real, tenant-scoped policy — beyond RLS turned on
            # with no policy (which fails closed, but isn't what this
            # platform is meant to do for legitimate tenant traffic).
            cur.execute(
                "select tablename, count(*) from pg_policies where tablename = any(%s) group by tablename",
                (list(validate_tenant_rls.EXPECTED_RLS_TABLES),),
            )
            policy_counts = dict(cur.fetchall())
            missing_policies = validate_tenant_rls.EXPECTED_RLS_TABLES - set(policy_counts)
            assert not missing_policies, f"tables missing an RLS policy: {sorted(missing_policies)}"
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.integration
def test_live_rls_runtime_matrix() -> None:
    from reliability.injectors.reachability import postgres_reachable

    tenant_scoped_url = "postgresql://platform_tenant_scoped:local-tenant-scoped-change-me@localhost:15432/data_platform"
    if not postgres_reachable(tenant_scoped_url):
        pytest.skip("no live PostgreSQL with the platform_tenant_scoped role reachable at localhost:15432")

    admin_url = "postgresql://platform_admin_bypass:local-admin-bypass-change-me@localhost:15432/data_platform"
    results = validate_tenant_rls.run_live_rls_matrix(tenant_scoped_url, admin_url, "tenant_demo", "tenant_enterprise")

    assert results["tenant_a_sees_only_own_rows"] is True
    assert results["tenant_a_denied_tenant_b_rows"] is True
    assert results["tenant_b_denied_tenant_a_rows"] is True
    assert results["fails_closed_with_no_tenant_context"] is True
    assert results["cross_tenant_insert_rejected"] is True
    assert results["admin_role_sees_multiple_tenants"] is True
    assert results["status"] == "passed"
