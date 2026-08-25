from __future__ import annotations

from typing import Any

from platform_shared.database import Postgres


class MetadataRepository:
    def __init__(self, postgres: Postgres) -> None:
        self.postgres = postgres

    async def list_tenants(self) -> list[dict[str, Any]]:
        return await self.postgres.fetch(
            """
            select tenant_id, tenant_name, plan, region, is_active, config, created_at, updated_at
            from tenant_config
            order by tenant_name
            """
        )

    async def upsert_tenant(self, tenant: dict[str, Any]) -> dict[str, Any]:
        return await self.postgres.fetchrow(
            """
            insert into tenant_config (tenant_id, tenant_name, plan, region, is_active, config, updated_at)
            values ($1,$2,$3,$4,$5,$6::jsonb,now())
            on conflict (tenant_id) do update set
                tenant_name = excluded.tenant_name,
                plan = excluded.plan,
                region = excluded.region,
                is_active = excluded.is_active,
                config = excluded.config,
                updated_at = now()
            returning tenant_id, tenant_name, plan, region, is_active, config, created_at, updated_at
            """,
            tenant["tenant_id"],
            tenant["tenant_name"],
            tenant["plan"],
            tenant["region"],
            tenant["is_active"],
            tenant["config_json"],
        )

    async def list_users(self, tenant_id: str) -> list[dict[str, Any]]:
        return await self.postgres.fetch(
            """
            select tenant_id, user_id, email, role, is_active, created_at
            from tenant_users
            where tenant_id = $1
            order by email
            """,
            tenant_id,
        )

    async def upsert_user(self, user: dict[str, Any]) -> dict[str, Any]:
        return await self.postgres.fetchrow(
            """
            insert into tenant_users (tenant_id, user_id, email, role, is_active)
            values ($1,$2,$3,$4,$5)
            on conflict (tenant_id, user_id) do update set
                email = excluded.email,
                role = excluded.role,
                is_active = excluded.is_active
            returning tenant_id, user_id, email, role, is_active, created_at
            """,
            user["tenant_id"],
            user["user_id"],
            user["email"],
            user["role"],
            user["is_active"],
        )

    async def list_products(self, tenant_id: str, limit: int, offset: int) -> list[dict[str, Any]]:
        return await self.postgres.fetch(
            """
            select tenant_id, product_id, sku, name, category, price, inventory_on_hand, active, updated_at
            from tenant_products
            where tenant_id = $1
            order by updated_at desc
            limit $2 offset $3
            """,
            tenant_id,
            limit,
            offset,
        )

