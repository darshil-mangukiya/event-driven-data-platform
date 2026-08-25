"""Redis-outage reliability exercise.

Points the real `platform_shared.cache.RedisCache` at a deliberately
unreachable address (a safe, non-destructive way to produce a genuine
connection failure without touching any real Redis instance) and proves
the platform's documented fallback actually executes: cache reads return
None (falls through to PostgreSQL), cache writes are a no-op, and rate
limiting fails open — none of it raises.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from reliability.injectors.asyncio_utils import run_coroutine
from reliability.injectors.reachability import redis_reachable
from reliability.models import ScenarioResult, StepResult
from spark.streaming.config import StreamingConfig

SCENARIO_ID = "redis-outage"

# Deliberately unreachable: RFC 5737 TEST-NET-1, a real routable-looking
# address nothing ever listens on in practice, port refused within ~1-2s.
UNREACHABLE_REDIS_URL = "redis://192.0.2.1:6379/0"


async def _exercise_cache() -> dict:
    from platform_shared.cache import RATE_LIMIT_UNKNOWN, RedisCache

    cache = RedisCache(UNREACHABLE_REDIS_URL, service_name="reliability-exercise")
    get_result = await cache.get_json("reliability:test-key")
    await cache.set_json("reliability:test-key", {"value": 1})  # must not raise
    rate_limit_result = await cache.incr_rate_limit("reliability:rate-limit-key")
    return {
        "get_json_result": get_result,
        "incr_rate_limit_result": rate_limit_result,
        "rate_limit_unknown_sentinel": RATE_LIMIT_UNKNOWN,
        "unavailable_count": cache.unavailable_count,
    }


def run(config: StreamingConfig | None = None) -> ScenarioResult:
    config = config or StreamingConfig()
    steps: list[StepResult] = []

    try:
        outcome = run_coroutine(asyncio.wait_for(_exercise_cache(), timeout=10))
        ok = (
            outcome["get_json_result"] is None
            and outcome["incr_rate_limit_result"] == outcome["rate_limit_unknown_sentinel"]
            and outcome["unavailable_count"] == 3  # get_json, set_json, incr_rate_limit each hit the outage path
        )
        steps.append(
            StepResult(
                name="exercise_redis_cache_against_unreachable_host",
                status="verified" if ok else "failed",
                detail=(
                    "RedisCache.get_json/set_json/incr_rate_limit all completed without raising and "
                    "returned their documented fallback values"
                    if ok
                    else f"fallback behavior did not match expectations: {outcome}"
                ),
                evidence=outcome,
            )
        )
    except Exception as exc:  # noqa: BLE001
        steps.append(
            StepResult(
                name="exercise_redis_cache_against_unreachable_host",
                status="failed",
                detail=f"RedisCache raised instead of degrading gracefully: {exc}",
            )
        )

    real_redis_up = redis_reachable("redis://localhost:6379/0")
    steps.append(
        StepResult(
            name="baseline_check_local_redis",
            status="verified",
            detail=f"local redis://localhost:6379 reachable={real_redis_up} (informational only, not part of the injected failure)",
            evidence={"reachable": real_redis_up},
        )
    )

    result = ScenarioResult(
        scenario_id=SCENARIO_ID,
        title="Redis Outage",
        component="platform_shared.cache.RedisCache / analytics-service rate limiting",
        expected_behavior=(
            "A Redis outage must not take the analytics API down: cache reads fall back to a "
            "miss (caller loads from PostgreSQL instead), cache writes are dropped silently, and "
            "rate limiting fails open rather than rejecting all traffic platform-wide."
        ),
        detection_method="platform_cache_events_total{outcome='unavailable'} increments; RedisCache.stats()['unavailable_count'] > 0.",
        impact="Without this fallback (the pre-fix behavior), every analytics-service request would 500 the moment Redis became unreachable — a full outage from a caching-layer failure.",
        root_cause="Network partition, Redis process crash/restart, or resource exhaustion on the Redis container.",
        recovery="No manual recovery needed — RedisCache re-establishes a connection automatically once Redis is reachable again (see test_cache_recovers_once_redis_is_reachable_again).",
        corrective_action=(
            "Added try/except fallback to RedisCache.get_json/set_json/incr_rate_limit and made "
            "analytics-service's enforce_rate_limit fail open on the RATE_LIMIT_UNKNOWN sentinel "
            "(services/shared/platform_shared/cache.py, services/analytics-service/app/main.py) — "
            "this exercise is what surfaced the gap."
        ),
        preventive_control="tests/test_tenant_access_and_cache.py asserts the fallback behavior on every CI run as well as during a manual drill.",
        steps=steps,
    )
    result.ended_at = datetime.now(timezone.utc)
    return result
