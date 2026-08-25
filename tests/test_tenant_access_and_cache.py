from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from platform_shared.auth import (
    TenantPrincipal,
    create_access_token,
    principal_from_headers,
    principal_from_token,
    require_tenant_access,
)
from platform_shared.cache import RATE_LIMIT_UNKNOWN, RedisCache, stable_cache_key


def test_tenant_principal_blocks_cross_tenant_access() -> None:
    principal = principal_from_headers(
        tenant_id="tenant_demo",
        user_id="analyst_demo",
        role="tenant_analyst",
    )

    assert principal.can_access("tenant_demo")
    assert not principal.can_access("tenant_enterprise")
    with pytest.raises(PermissionError):
        require_tenant_access(principal, "tenant_enterprise")


def test_platform_admin_can_access_all_tenants() -> None:
    principal = TenantPrincipal(user_id="platform_admin", tenant_id="tenant_demo", role="platform_admin")

    assert principal.can_access("tenant_enterprise")


def test_cache_key_is_stable_and_tenant_scoped() -> None:
    params_a = {"limit": 10, "start_date": "2026-01-01"}
    params_b = {"start_date": "2026-01-01", "limit": 10}

    assert stable_cache_key("metrics:revenue", "tenant_demo", params_a) == stable_cache_key(
        "metrics:revenue", "tenant_demo", params_b
    )
    assert stable_cache_key("metrics:revenue", "tenant_demo", params_a) != stable_cache_key(
        "metrics:revenue", "tenant_enterprise", params_a
    )


def test_jwt_round_trip_preserves_tenant_role_and_scopes() -> None:
    principal = TenantPrincipal(
        user_id="analyst_demo",
        tenant_id="tenant_demo",
        role="tenant_analyst",
        scopes=("metrics:read", "events:write"),
    )

    token = create_access_token(principal, expires_in_seconds=300)
    restored = principal_from_token(token)

    assert restored.user_id == "analyst_demo"
    assert restored.tenant_id == "tenant_demo"
    assert restored.role == "tenant_analyst"
    assert restored.has_scope("metrics:read")


# -- Redis-outage reliability behavior (see docs/reliability.md) -----------


def _cache_with_broken_client() -> RedisCache:
    cache = RedisCache("redis://localhost:6379/0", service_name="test")
    broken_client = AsyncMock()
    broken_client.get.side_effect = ConnectionError("redis unavailable")
    broken_client.set.side_effect = ConnectionError("redis unavailable")
    broken_client.incr.side_effect = ConnectionError("redis unavailable")
    cache._client = broken_client  # bypass connect(); simulates an outage after a prior successful connect
    return cache


@pytest.mark.asyncio
async def test_get_json_falls_back_to_none_when_redis_unavailable() -> None:
    cache = _cache_with_broken_client()
    result = await cache.get_json("some-key")
    assert result is None
    assert cache.unavailable_count == 1


@pytest.mark.asyncio
async def test_set_json_does_not_raise_when_redis_unavailable() -> None:
    cache = _cache_with_broken_client()
    # Must not raise: a cache write failing during an outage must not fail the request.
    await cache.set_json("some-key", {"value": 1})
    assert cache.unavailable_count == 1


@pytest.mark.asyncio
async def test_incr_rate_limit_returns_unknown_sentinel_when_redis_unavailable() -> None:
    cache = _cache_with_broken_client()
    result = await cache.incr_rate_limit("rate-limit-key")
    assert result == RATE_LIMIT_UNKNOWN
    assert cache.unavailable_count == 1


@pytest.mark.asyncio
async def test_cache_recovers_once_redis_is_reachable_again() -> None:
    cache = _cache_with_broken_client()
    assert await cache.get_json("k") is None  # outage

    healthy_client = AsyncMock()
    healthy_client.get.return_value = None
    cache._client = healthy_client  # simulate Redis coming back
    result = await cache.get_json("k")
    assert result is None  # a real cache miss now, not a fallback
    assert cache.misses == 1
    assert cache.unavailable_count == 1  # unchanged since recovery
