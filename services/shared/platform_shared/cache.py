"""Redis-backed cache with a documented, tested outage fallback.

Reliability requirement (see docs/reliability.md, "redis-outage" exercise):
a Redis outage must not take the analytics API down with it. Every method
that talks to Redis is wrapped so a connection failure is caught, logged,
counted (`record_cache_event(..., outcome="unavailable")`), and turned into
a safe fallback value instead of propagating as an exception:

* ``get_json`` returns ``None`` (a cache miss) on failure — the caller's
  normal "miss -> load from PostgreSQL" path already exists and just runs,
  so a metrics API request still succeeds, just uncached.
* ``set_json`` becomes a no-op — losing a cache write during an outage is
  fine; blocking or failing the request over it is not.
* ``incr_rate_limit`` returns ``-1`` (rather than raising) to signal
  "rate-limit state unknown." Callers must explicitly check for ``-1`` and
  choose to fail *open* (allow the request) rather than fail closed and
  reject all traffic platform-wide because the cache is down — see
  ``services/analytics-service/app/main.py::enforce_rate_limit``. This is a
  deliberate availability-over-strictness tradeoff for a demo-scale
  platform; documented, not accidental.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from platform_shared.metrics import record_cache_availability, record_cache_event

LOGGER = logging.getLogger(__name__)

RATE_LIMIT_UNKNOWN = -1


def stable_cache_key(prefix: str, tenant_id: str, params: dict[str, Any]) -> str:
    normalized = json.dumps(params, sort_keys=True, default=str)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:tenant:{tenant_id}:{digest}"


class RedisCache:
    def __init__(
        self,
        redis_url: str,
        default_ttl_seconds: int = 120,
        service_name: str = "unknown",
        socket_connect_timeout_seconds: float = 2.0,
        socket_timeout_seconds: float = 2.0,
    ) -> None:
        self.redis_url = redis_url
        self.default_ttl_seconds = default_ttl_seconds
        self.service_name = service_name
        self.socket_connect_timeout_seconds = socket_connect_timeout_seconds
        self.socket_timeout_seconds = socket_timeout_seconds
        self._client: Any | None = None
        self.hits = 0
        self.misses = 0
        self.unavailable_count = 0

    async def connect(self) -> None:
        if self._client is None:
            import redis.asyncio as redis

            # Explicit socket timeouts matter as much as the try/except
            # fallback below: without them, a host that's unreachable but
            # not actively refusing connections (a network partition, a
            # security-group drop, a blackholed address) can hang far
            # longer than any caller expects instead of failing fast into
            # the fallback path. Discovered by the redis-outage reliability
            # exercise, which pointed this at a deliberately unreachable
            # address and found the *lack* of a timeout, not the fallback
            # logic itself, was the actual bug.
            self._client = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=self.socket_connect_timeout_seconds,
                socket_timeout=self.socket_timeout_seconds,
            )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _record_unavailable(self, operation: str, exc: Exception) -> None:
        self.unavailable_count += 1
        record_cache_event(self.service_name, "redis", "unavailable")
        record_cache_availability(self.service_name, "redis", available=False)
        LOGGER.warning(
            "redis unavailable during %s; falling back",
            operation,
            extra={"cache": "redis", "operation": operation, "error": str(exc)},
        )

    async def get_json(self, key: str) -> Any | None:
        try:
            await self.connect()
            assert self._client is not None
            value = await self._client.get(key)
        except Exception as exc:  # noqa: BLE001 - any redis/connection error triggers fallback
            self._record_unavailable("get_json", exc)
            return None
        record_cache_availability(self.service_name, "redis", available=True)
        if value is None:
            self.misses += 1
            record_cache_event(self.service_name, "redis", "miss")
            return None
        self.hits += 1
        record_cache_event(self.service_name, "redis", "hit")
        return json.loads(value)

    async def set_json(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        try:
            await self.connect()
            assert self._client is not None
            record_cache_event(self.service_name, "redis", "set")
            await self._client.set(key, json.dumps(value, default=str), ex=ttl_seconds or self.default_ttl_seconds)
            record_cache_availability(self.service_name, "redis", available=True)
        except Exception as exc:  # noqa: BLE001
            self._record_unavailable("set_json", exc)

    async def incr_rate_limit(self, key: str, ttl_seconds: int = 60) -> int:
        try:
            await self.connect()
            assert self._client is not None
            current = await self._client.incr(key)
            record_cache_event(self.service_name, "redis", "rate_limit_incr")
            if current == 1:
                await self._client.expire(key, ttl_seconds)
            record_cache_availability(self.service_name, "redis", available=True)
            return int(current)
        except Exception as exc:  # noqa: BLE001
            self._record_unavailable("incr_rate_limit", exc)
            return RATE_LIMIT_UNKNOWN

    def stats(self) -> dict[str, Any]:
        total = self.hits + self.misses
        hit_rate = self.hits / total if total else 0
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": round(hit_rate, 4),
            "unavailable_count": self.unavailable_count,
        }
