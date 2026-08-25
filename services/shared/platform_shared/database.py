from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class QueryWindow:
    start_date: date | None = None
    end_date: date | None = None

    def as_sql(self, column_name: str, first_param: int = 1) -> tuple[str, list[date]]:
        clauses: list[str] = []
        values: list[date] = []
        next_param = first_param
        if self.start_date:
            clauses.append(f"{column_name} >= ${next_param}")
            values.append(self.start_date)
            next_param += 1
        if self.end_date:
            clauses.append(f"{column_name} <= ${next_param}")
            values.append(self.end_date)
        return (" AND ".join(clauses), values)


class Postgres:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._pool: Any | None = None

    async def connect(self) -> None:
        if self._pool is None:
            import asyncpg

            self._pool = await asyncpg.create_pool(
                dsn=self.database_url,
                min_size=1,
                max_size=10,
                command_timeout=30,
            )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, *args)
            return [dict(row) for row in rows]

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(query, *args)
            return dict(row) if row else None

    async def execute(self, query: str, *args: Any) -> str:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as connection:
            return await connection.execute(query, *args)

    async def executemany(self, query: str, args: Sequence[Sequence[Any]]) -> None:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as connection:
            await connection.executemany(query, args)

    async def execute_transaction(self, statements: Sequence[tuple[str, Sequence[Any]]]) -> list[str]:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                statuses: list[str] = []
                for query, args in statements:
                    statuses.append(await connection.execute(query, *args))
                return statuses

    # -- Tenant-scoped helpers -------------------------------------------
    #
    # These set PostgreSQL's `app.tenant_id` session variable — the value
    # `database/security/tenant_rls.sql`'s row-level-security policies key
    # on via `current_app_tenant_id()` — using `set_config(..., true)`.
    # The third argument (`is_local`) makes the setting transaction-local,
    # equivalent to `SET LOCAL`: it only exists for the lifetime of the
    # single transaction opened by `connection.transaction()` below and is
    # automatically discarded when that transaction commits or rolls back,
    # *before* the connection is released back to the pool. This is what
    # makes it safe to use with a pooled connection — a later request that
    # happens to reuse the same physical connection cannot inherit this
    # request's tenant context, because by the time the connection is
    # returned to the pool the setting no longer exists on it. Runtime
    # coverage of this property lives in
    # `tests/test_tenant_access_and_cache.py`.
    #
    # Enforcement of the tenant boundary implied by this context still
    # depends on the connection's role being non-superuser and
    # NOBYPASSRLS (see `platform_tenant_scoped` in
    # `database/security/tenant_rls.sql`) — these helpers only get the
    # tenant identity to PostgreSQL, they do not by themselves change what
    # the connected role is allowed to bypass.
    async def fetch_scoped(self, tenant_id: str, query: str, *args: Any) -> list[dict[str, Any]]:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute("select set_config('app.tenant_id', $1, true)", tenant_id)
                rows = await connection.fetch(query, *args)
                return [dict(row) for row in rows]

    async def fetchrow_scoped(self, tenant_id: str, query: str, *args: Any) -> dict[str, Any] | None:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute("select set_config('app.tenant_id', $1, true)", tenant_id)
                row = await connection.fetchrow(query, *args)
                return dict(row) if row else None

    async def execute_scoped(self, tenant_id: str, query: str, *args: Any) -> str:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute("select set_config('app.tenant_id', $1, true)", tenant_id)
                return await connection.execute(query, *args)
