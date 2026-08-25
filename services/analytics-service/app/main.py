from __future__ import annotations

import time
from datetime import date
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from platform_shared.auth import TenantPrincipal, principal_from_authorization
from platform_shared.cache import RATE_LIMIT_UNKNOWN, RedisCache, stable_cache_key
from platform_shared.config import service_settings
from platform_shared.database import Postgres
from platform_shared.logging import configure_logging
from platform_shared.metrics import InMemoryServiceMetrics, MetricsMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel
from starlette.responses import Response

from app.repository import AnalyticsRepository

SERVICE_NAME = "analytics-service"
settings = service_settings(SERVICE_NAME)
configure_logging(SERVICE_NAME, settings.log_level)
request_metrics = InMemoryServiceMetrics()
postgres = Postgres(settings.database_url)
cache = RedisCache(settings.redis_url, settings.default_cache_ttl_seconds, service_name=SERVICE_NAME)
repository = AnalyticsRepository(postgres)

app = FastAPI(
    title="Data Platform Analytics Service",
    description="Tenant-scoped analytics data product API backed by PostgreSQL serving tables and Redis cache.",
    version="0.1.0",
)
app.add_middleware(MetricsMiddleware, metrics=request_metrics, service_name=SERVICE_NAME)


@app.middleware("http")
async def api_usage_logging(request: Request, call_next: Any) -> Any:
    started = time.perf_counter()
    response = await call_next(request)
    if request.url.path not in {"/health", "/metrics"}:
        tenant_id = request.query_params.get("tenant_id") or request.headers.get("X-Tenant-ID") or "unknown"
        latency_ms = (time.perf_counter() - started) * 1000
        try:
            await postgres.execute(
                """
                insert into api_usage_log (
                    tenant_id, user_id, endpoint, status_code, latency_ms, cache_status,
                    role, trace_id, requested_at
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8,now())
                """,
                tenant_id,
                request.headers.get("X-User-ID"),
                request.url.path,
                response.status_code,
                latency_ms,
                response.headers.get("X-Cache", "unknown"),
                request.headers.get("X-User-Role"),
                request.headers.get("X-Trace-ID"),
            )
        except Exception:
            pass
    return response


class MetricResponse(BaseModel):
    tenant_id: str
    metric: str
    cached: bool
    count: int
    data: list[dict[str, Any]]


class HealthScoreResponse(BaseModel):
    tenant_id: str
    tenant_health_score: float | int
    payment_successes: int | None = None
    payment_failures: int | None = None
    events_processed: int | None = None


def get_principal(
    authorization: str | None = Header(default=None),
    x_tenant_id: str | None = Header(default=None),
    x_user_id: str | None = Header(default=None),
    x_user_role: str | None = Header(default=None),
) -> TenantPrincipal:
    try:
        return principal_from_authorization(
            authorization=authorization,
            tenant_id=x_tenant_id,
            user_id=x_user_id,
            role=x_user_role,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc


def assert_tenant_scope(principal: TenantPrincipal, tenant_id: str) -> None:
    if not principal.can_access(tenant_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"user {principal.user_id} cannot access tenant {tenant_id}",
        )


async def enforce_rate_limit(request: Request, principal: TenantPrincipal) -> None:
    key = f"rate_limit:{principal.tenant_id}:{principal.user_id}:{request.url.path}"
    current = await cache.incr_rate_limit(key)
    if current == RATE_LIMIT_UNKNOWN:
        # Redis is unavailable (see RedisCache._record_unavailable) — fail
        # open rather than reject all traffic platform-wide because the
        # cache is down. Documented tradeoff: rate limiting is best-effort
        # during a cache outage, availability of the analytics API is not.
        return
    if current > settings.rate_limit_requests_per_minute:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="rate limit exceeded")


async def cached_metric(
    *,
    metric: str,
    tenant_id: str,
    params: dict[str, Any],
    loader: Any,
) -> MetricResponse:
    key = stable_cache_key(f"metrics:{metric}", tenant_id, params)
    cached = await cache.get_json(key)
    if cached is not None:
        return MetricResponse(tenant_id=tenant_id, metric=metric, cached=True, count=len(cached), data=cached)

    data = await loader()
    await cache.set_json(key, data)
    return MetricResponse(tenant_id=tenant_id, metric=metric, cached=False, count=len(data), data=data)


@app.on_event("startup")
async def startup() -> None:
    await postgres.connect()
    await cache.connect()


@app.on_event("shutdown")
async def shutdown() -> None:
    await postgres.close()
    await cache.close()


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"service": SERVICE_NAME, "status": "ok", "environment": settings.environment}


@app.get("/metrics")
async def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/metrics/revenue", response_model=MetricResponse)
async def revenue(
    request: Request,
    tenant_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(default=30, ge=1, le=365),
    offset: int = Query(default=0, ge=0),
    principal: TenantPrincipal = Depends(get_principal),
) -> MetricResponse:
    assert_tenant_scope(principal, tenant_id)
    await enforce_rate_limit(request, principal)
    params = locals() | {"request": None, "principal": None}
    return await cached_metric(
        metric="revenue",
        tenant_id=tenant_id,
        params=params,
        loader=lambda: repository.revenue(
            tenant_id=tenant_id, start_date=start_date, end_date=end_date, limit=limit, offset=offset
        ),
    )


@app.get("/metrics/customers", response_model=MetricResponse)
async def customers(
    request: Request,
    tenant_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(default=30, ge=1, le=365),
    offset: int = Query(default=0, ge=0),
    principal: TenantPrincipal = Depends(get_principal),
) -> MetricResponse:
    assert_tenant_scope(principal, tenant_id)
    await enforce_rate_limit(request, principal)
    params = locals() | {"request": None, "principal": None}
    return await cached_metric(
        metric="customers",
        tenant_id=tenant_id,
        params=params,
        loader=lambda: repository.customers(
            tenant_id=tenant_id, start_date=start_date, end_date=end_date, limit=limit, offset=offset
        ),
    )


@app.get("/metrics/churn", response_model=MetricResponse)
async def churn(
    request: Request,
    tenant_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(default=30, ge=1, le=365),
    offset: int = Query(default=0, ge=0),
    principal: TenantPrincipal = Depends(get_principal),
) -> MetricResponse:
    assert_tenant_scope(principal, tenant_id)
    await enforce_rate_limit(request, principal)
    params = locals() | {"request": None, "principal": None}
    return await cached_metric(
        metric="churn",
        tenant_id=tenant_id,
        params=params,
        loader=lambda: repository.churn(
            tenant_id=tenant_id, start_date=start_date, end_date=end_date, limit=limit, offset=offset
        ),
    )


@app.get("/metrics/retention", response_model=MetricResponse)
async def retention(
    request: Request,
    tenant_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(default=30, ge=1, le=365),
    offset: int = Query(default=0, ge=0),
    principal: TenantPrincipal = Depends(get_principal),
) -> MetricResponse:
    assert_tenant_scope(principal, tenant_id)
    await enforce_rate_limit(request, principal)
    params = locals() | {"request": None, "principal": None}
    return await cached_metric(
        metric="retention",
        tenant_id=tenant_id,
        params=params,
        loader=lambda: repository.retention(
            tenant_id=tenant_id, start_date=start_date, end_date=end_date, limit=limit, offset=offset
        ),
    )


@app.get("/metrics/marketing_roi", response_model=MetricResponse)
async def marketing_roi(
    request: Request,
    tenant_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: TenantPrincipal = Depends(get_principal),
) -> MetricResponse:
    assert_tenant_scope(principal, tenant_id)
    await enforce_rate_limit(request, principal)
    params = locals() | {"request": None, "principal": None}
    return await cached_metric(
        metric="marketing_roi",
        tenant_id=tenant_id,
        params=params,
        loader=lambda: repository.marketing_roi(
            tenant_id=tenant_id, start_date=start_date, end_date=end_date, limit=limit, offset=offset
        ),
    )


@app.get("/metrics/product_performance", response_model=MetricResponse)
async def product_performance(
    request: Request,
    tenant_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: TenantPrincipal = Depends(get_principal),
) -> MetricResponse:
    assert_tenant_scope(principal, tenant_id)
    await enforce_rate_limit(request, principal)
    params = locals() | {"request": None, "principal": None}
    return await cached_metric(
        metric="product_performance",
        tenant_id=tenant_id,
        params=params,
        loader=lambda: repository.product_performance(
            tenant_id=tenant_id, start_date=start_date, end_date=end_date, limit=limit, offset=offset
        ),
    )


@app.get("/metrics/payment_success", response_model=MetricResponse)
async def payment_success(
    request: Request,
    tenant_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(default=30, ge=1, le=365),
    offset: int = Query(default=0, ge=0),
    principal: TenantPrincipal = Depends(get_principal),
) -> MetricResponse:
    assert_tenant_scope(principal, tenant_id)
    await enforce_rate_limit(request, principal)
    params = locals() | {"request": None, "principal": None}
    return await cached_metric(
        metric="payment_success",
        tenant_id=tenant_id,
        params=params,
        loader=lambda: repository.payment_success(
            tenant_id=tenant_id, start_date=start_date, end_date=end_date, limit=limit, offset=offset
        ),
    )


@app.get("/metrics/event_throughput", response_model=MetricResponse)
async def event_throughput(
    request: Request,
    tenant_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(default=30, ge=1, le=365),
    offset: int = Query(default=0, ge=0),
    principal: TenantPrincipal = Depends(get_principal),
) -> MetricResponse:
    assert_tenant_scope(principal, tenant_id)
    await enforce_rate_limit(request, principal)
    params = locals() | {"request": None, "principal": None}
    return await cached_metric(
        metric="event_throughput",
        tenant_id=tenant_id,
        params=params,
        loader=lambda: repository.event_throughput(
            tenant_id=tenant_id, start_date=start_date, end_date=end_date, limit=limit, offset=offset
        ),
    )


@app.get("/metrics/tenant_health_score", response_model=HealthScoreResponse)
async def tenant_health_score(
    tenant_id: str,
    principal: TenantPrincipal = Depends(get_principal),
) -> dict[str, Any]:
    assert_tenant_scope(principal, tenant_id)
    return await repository.tenant_health_score(tenant_id=tenant_id)


@app.get("/alerts", response_model=MetricResponse)
async def alerts(
    request: Request,
    tenant_id: str,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    principal: TenantPrincipal = Depends(get_principal),
) -> MetricResponse:
    assert_tenant_scope(principal, tenant_id)
    await enforce_rate_limit(request, principal)
    data = await repository.alerts(tenant_id=tenant_id, limit=limit, offset=offset)
    return MetricResponse(tenant_id=tenant_id, metric="alerts", cached=False, count=len(data), data=data)


@app.get("/tenants")
async def tenants(principal: TenantPrincipal = Depends(get_principal)) -> list[dict[str, Any]]:
    if not principal.is_platform_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="platform admin role required")
    return await repository.tenants()


@app.get("/system/status")
async def system_status(
    tenant_id: str,
    principal: TenantPrincipal = Depends(get_principal),
) -> dict[str, Any]:
    assert_tenant_scope(principal, tenant_id)
    status_payload = await repository.system_status(tenant_id=tenant_id)
    return {
        "service": SERVICE_NAME,
        "status": "ok",
        "cache": cache.stats(),
        "request_metrics": request_metrics.snapshot(),
        **status_payload,
    }
