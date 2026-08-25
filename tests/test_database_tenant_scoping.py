"""Tests for the P6 "Final Application RLS + Validator Closeout" pass.

Previously, `platform_shared.database.Postgres` had no concept of tenant
context at all: every service that queried a tenant-scoped table relied
solely on its own `WHERE tenant_id = $1` clause, and every service's
runtime PostgreSQL connection was the `platform` role — a PostgreSQL
SUPERUSER, which bypasses row-level security unconditionally regardless of
any policy defined in `database/security/tenant_rls.sql`. RLS existed and
was correct, but nothing at runtime actually depended on it.

`Postgres.fetch_scoped`/`fetchrow_scoped`/`execute_scoped` close that gap:
they set PostgreSQL's `app.tenant_id` session variable — transaction-local,
via `set_config(..., true)` — before running the query, on the exact same
physical connection, inside the exact same transaction. This file tests:

1. That the scoped helpers actually issue `set_config(..., true)` before
   the real query, inside a transaction (unit-level, mocked asyncpg pool,
   no live database needed).
2. That a pooled connection cannot leak one request's tenant context into
   a later, unrelated request that happens to reuse the same physical
   connection (live, `@pytest.mark.integration`, skips cleanly without a
   reachable database).
3. That the checked-in Docker Compose / Kubernetes / Helm configuration
   does not default any tenant-facing service back to the `platform`
   PostgreSQL superuser (static, always runs).

See `evidence/validation/application-rls-runtime-verification.md` for the
full trace.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from platform_shared.database import Postgres

PROJECT_ROOT = Path(__file__).resolve().parents[1]


# --- 1. Scoped helpers actually set tenant context, transaction-local ----


class _FakeTransactionCtx:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def __aenter__(self) -> None:
        self._calls.append("transaction.__aenter__")

    async def __aexit__(self, *exc: object) -> None:
        self._calls.append("transaction.__aexit__")


class _FakeConnection:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    def transaction(self) -> _FakeTransactionCtx:
        return _FakeTransactionCtx(self._calls)

    async def execute(self, query: str, *args: object) -> str:
        self._calls.append(f"execute:{query.strip().splitlines()[0].strip()}:{args!r}")
        return "SET"

    async def fetch(self, query: str, *args: object) -> list[object]:
        self._calls.append(f"fetch:{query.strip().splitlines()[0].strip()}:{args!r}")
        return []

    async def fetchrow(self, query: str, *args: object) -> object | None:
        self._calls.append(f"fetchrow:{query.strip().splitlines()[0].strip()}:{args!r}")
        return None


class _FakeAcquireCtx:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _FakeConnection:
        return self._connection

    async def __aexit__(self, *exc: object) -> None:
        return None


def _postgres_with_fake_pool() -> tuple[Postgres, list[str]]:
    calls: list[str] = []
    connection = _FakeConnection(calls)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=_FakeAcquireCtx(connection))
    pg = Postgres("postgresql://unused:unused@localhost/unused")
    pg._pool = pool
    pg.connect = AsyncMock()  # already "connected" — skip real asyncpg.create_pool
    return pg, calls


@pytest.mark.asyncio
async def test_fetch_scoped_sets_tenant_context_before_query_inside_a_transaction() -> None:
    pg, calls = _postgres_with_fake_pool()

    await pg.fetch_scoped("tenant_demo", "select 1 from raw_events where tenant_id = $1", "tenant_demo")

    assert calls == [
        "transaction.__aenter__",
        "execute:select set_config('app.tenant_id', $1, true):('tenant_demo',)",
        "fetch:select 1 from raw_events where tenant_id = $1:('tenant_demo',)",
        "transaction.__aexit__",
    ]


@pytest.mark.asyncio
async def test_fetchrow_scoped_sets_tenant_context_before_query_inside_a_transaction() -> None:
    pg, calls = _postgres_with_fake_pool()

    await pg.fetchrow_scoped("tenant_enterprise", "select 1 from alerts where tenant_id = $1", "tenant_enterprise")

    assert calls == [
        "transaction.__aenter__",
        "execute:select set_config('app.tenant_id', $1, true):('tenant_enterprise',)",
        "fetchrow:select 1 from alerts where tenant_id = $1:('tenant_enterprise',)",
        "transaction.__aexit__",
    ]


@pytest.mark.asyncio
async def test_execute_scoped_sets_tenant_context_before_query_inside_a_transaction() -> None:
    pg, calls = _postgres_with_fake_pool()

    await pg.execute_scoped(
        "tenant_demo",
        "insert into processed_orders (tenant_id, order_id) values ($1, $2)",
        "tenant_demo",
        "order-1",
    )

    assert calls == [
        "transaction.__aenter__",
        "execute:select set_config('app.tenant_id', $1, true):('tenant_demo',)",
        "execute:insert into processed_orders (tenant_id, order_id) values ($1, $2):('tenant_demo', 'order-1')",
        "transaction.__aexit__",
    ]
    # The tenant-context set_config call and the real write happen inside
    # the *same* transaction (one __aenter__/__aexit__ pair, not two) —
    # this is what makes `set_config(..., true)`'s transaction-local
    # ("is_local") behavior equivalent to `SET LOCAL` here.
    assert calls.count("transaction.__aenter__") == 1
    assert calls.count("transaction.__aexit__") == 1


# --- 2. Pooled-connection tenant-context leakage (live) ------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pooled_connection_does_not_leak_tenant_context_between_requests() -> None:
    """Verify that pooled connections do not retain tenant context.

    Forces a single-connection pool (min_size=max_size=1) so two
    sequential `fetch_scoped` calls are guaranteed to reuse the exact same
    physical PostgreSQL connection — the scenario where leakage would be
    possible if tenant context were set with a session-level `SET`
    instead of a transaction-local `set_config(..., true)`. Proves the
    second call's connection has no `app.tenant_id` set from the first
    call before its own transaction starts.
    """
    from reliability.injectors.reachability import postgres_reachable

    tenant_scoped_url = "postgresql://platform_tenant_scoped:local-tenant-scoped-change-me@localhost:15432/data_platform"
    if not postgres_reachable(tenant_scoped_url):
        pytest.skip("no live PostgreSQL with the platform_tenant_scoped role reachable at localhost:15432")

    import asyncpg

    pg = Postgres(tenant_scoped_url)
    pg._pool = await asyncpg.create_pool(dsn=tenant_scoped_url, min_size=1, max_size=1, command_timeout=30)
    try:
        # Request 1: tenant_demo context, real query against a real table.
        await pg.fetch_scoped("tenant_demo", "select 1 from raw_events where tenant_id = $1", "tenant_demo")

        # Request 2 (same pooled connection, since max_size=1): before this
        # request's own fetch_scoped sets anything, the leftover state from
        # request 1 must already be gone.
        async with pg._pool.acquire() as connection:
            leftover = await connection.fetchval("select current_setting('app.tenant_id', true)")
        assert leftover in (None, ""), (
            f"tenant context leaked across a pooled connection between requests: "
            f"expected no leftover app.tenant_id, found {leftover!r}"
        )

        # And a fresh request with a different tenant on the same
        # physical connection works correctly and independently.
        await pg.fetch_scoped("tenant_enterprise", "select 1 from raw_events where tenant_id = $1", "tenant_enterprise")
        async with pg._pool.acquire() as connection:
            leftover_again = await connection.fetchval("select current_setting('app.tenant_id', true)")
        assert leftover_again in (None, "")
    finally:
        await pg.close()


# --- 2b. Defense-in-depth / cross-tenant write rejection (live) ----------
#
# Permanent, checked-in versions of the ad hoc proofs run manually during
# verification — see evidence/validation/application-rls-runtime-verification.md
# for the original live output these reproduce.


@pytest.mark.integration
@pytest.mark.asyncio
async def test_same_sql_different_session_tenant_context_yields_different_rows() -> None:
    """Database-layer check: same SQL text, same WHERE clause
    (tenant_id = 'tenant_demo'), called directly (bypassing the API layer
    entirely) as platform_tenant_scoped. Only the PostgreSQL session's
    app.tenant_id changes between the two calls — if RLS were not
    independently enforced, both would return tenant_demo's real rows.
    """
    from reliability.injectors.reachability import postgres_reachable

    tenant_scoped_url = "postgresql://platform_tenant_scoped:local-tenant-scoped-change-me@localhost:15432/data_platform"
    if not postgres_reachable(tenant_scoped_url):
        pytest.skip("no live PostgreSQL with the platform_tenant_scoped role reachable at localhost:15432")

    pg = Postgres(tenant_scoped_url)
    query = "select tenant_id from tenant_metrics_daily where tenant_id = $1 limit 1"
    try:
        matched = await pg.fetch_scoped("tenant_demo", query, "tenant_demo")
        mismatched = await pg.fetch_scoped("tenant_enterprise", query, "tenant_demo")
        no_context = await pg.fetch("select 1 from tenant_metrics_daily where tenant_id = 'tenant_demo' limit 1")

        assert len(matched) >= 1, "expected real rows when session context matches the WHERE clause"
        assert mismatched == [], (
            "RLS did not independently enforce the tenant boundary: a mismatched "
            "session context still returned rows matched only by the WHERE clause"
        )
        assert no_context == [], "expected RLS to fail closed with no tenant context set at all"
    finally:
        await pg.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_processing_repository_write_with_mismatched_tenant_context_is_rejected() -> None:
    """Verify PostgreSQL's own WITH CHECK clause (not application validation)
    rejects a write whose row tenant_id doesn't match the session's RLS
    context, using the real Postgres.execute_scoped() code path.
    """
    import uuid

    import asyncpg

    from reliability.injectors.reachability import postgres_reachable

    tenant_scoped_url = "postgresql://platform_tenant_scoped:local-tenant-scoped-change-me@localhost:15432/data_platform"
    if not postgres_reachable(tenant_scoped_url):
        pytest.skip("no live PostgreSQL with the platform_tenant_scoped role reachable at localhost:15432")

    pg = Postgres(tenant_scoped_url)
    try:
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await pg.execute_scoped(
                "tenant_enterprise",  # session context claims tenant_enterprise
                "insert into alerts (tenant_id, alert_type, severity, status, message) values ($1,$2,$3,$4,$5)",
                "tenant_demo",  # ...but the row itself claims tenant_demo -> WITH CHECK rejects it
                "proof_alert",
                "low",
                "open",
                f"pool-safety regression test {uuid.uuid4().hex[:8]}",
            )
    finally:
        await pg.close()


# --- 3. Static config audit: no tenant-facing service defaults to `platform` --


CORE_TENANT_FACING_SERVICES = ("analytics-service", "processing-service")
COMPOSE_TENANT_FACING_SERVICES = (*CORE_TENANT_FACING_SERVICES, "demo-dashboard")


def test_docker_compose_does_not_default_tenant_facing_services_to_platform_superuser() -> None:
    compose = (PROJECT_ROOT / "docker-compose.yml").read_text()
    # Split into per-service blocks on the two-space top-level service key.
    blocks = re.split(r"\n  (?=[a-zA-Z0-9_-]+:\n)", compose)
    found = set()
    for block in blocks:
        for service in COMPOSE_TENANT_FACING_SERVICES:
            if block.startswith(f"{service}:") or block.startswith(f"  {service}:"):
                found.add(service)
                match = re.search(r"DATABASE_URL:\s*(\S+)", block)
                assert match is not None, f"{service}: no explicit DATABASE_URL override found in docker-compose.yml"
                url = match.group(1)
                assert "platform_tenant_scoped:" in url, (
                    f"{service}: DATABASE_URL does not use the non-superuser platform_tenant_scoped role: {url!r}"
                )
                assert not url.startswith("postgresql://platform:"), (
                    f"{service}: DATABASE_URL still defaults to the platform superuser: {url!r}"
                )
    assert found == set(COMPOSE_TENANT_FACING_SERVICES), (
        f"expected to find blocks for {COMPOSE_TENANT_FACING_SERVICES}, found {found}"
    )


def test_kubernetes_manifest_does_not_default_tenant_facing_services_to_platform_superuser() -> None:
    manifest = (PROJECT_ROOT / "deploy" / "kubernetes" / "base" / "20-services.yaml").read_text()
    for service in CORE_TENANT_FACING_SERVICES:
        # Isolate this Deployment's container block up to the next '---' doc separator.
        start = manifest.index(f"name: {service}\n")
        end = manifest.index("\n---", start)
        block = manifest[start:end]
        match = re.search(r'value:\s*"([^"]+)"', block)
        assert match is not None, f"{service}: no explicit DATABASE_URL env override found in 20-services.yaml"
        url = match.group(1)
        assert "platform_tenant_scoped:" in url, f"{service}: k8s DATABASE_URL is not platform_tenant_scoped: {url!r}"
        assert not url.startswith("postgresql://platform:"), f"{service}: k8s DATABASE_URL still uses the superuser: {url!r}"


def test_helm_chart_does_not_default_tenant_facing_services_to_platform_superuser() -> None:
    template = (PROJECT_ROOT / "deploy" / "helm" / "cloudscale" / "templates" / "20-services.yaml").read_text()
    assert 'eq $key "analytics"' in template
    assert 'eq $key "processing"' in template
    assert "tenantScopedUser" in template
    assert "adminBypassUser" in template
    values = (PROJECT_ROOT / "deploy" / "helm" / "cloudscale" / "values.yaml").read_text()
    assert "tenantScopedUser: platform_tenant_scoped" in values
    assert "adminBypassUser: platform_admin_bypass" in values
