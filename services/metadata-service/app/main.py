from __future__ import annotations

import json
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from platform_shared.auth import (
    ANALYST_ROLE,
    TenantPrincipal,
    create_access_token,
    principal_from_authorization,
)
from platform_shared.config import service_settings
from platform_shared.database import Postgres
from platform_shared.logging import configure_logging
from platform_shared.metrics import InMemoryServiceMetrics, MetricsMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, EmailStr, Field
from starlette.responses import Response

from app.repository import MetadataRepository

SERVICE_NAME = "metadata-service"
settings = service_settings(SERVICE_NAME)
configure_logging(SERVICE_NAME, settings.log_level)
request_metrics = InMemoryServiceMetrics()
postgres = Postgres(settings.database_url)
repository = MetadataRepository(postgres)

app = FastAPI(
    title="Data Platform Metadata Service",
    description="Tenant metadata, product reference data, and RBAC mapping service for the analytics platform.",
    version="0.1.0",
)
app.add_middleware(MetricsMiddleware, metrics=request_metrics, service_name=SERVICE_NAME)


class TenantUpsertRequest(BaseModel):
    tenant_id: str = Field(min_length=2)
    tenant_name: str = Field(min_length=2)
    plan: str = Field(default="growth")
    region: str = Field(default="us")
    is_active: bool = True
    config: dict[str, Any] = Field(default_factory=dict)


class UserUpsertRequest(BaseModel):
    tenant_id: str
    user_id: str
    email: EmailStr
    role: str = Field(default=ANALYST_ROLE)
    is_active: bool = True


class TokenIssueRequest(BaseModel):
    tenant_id: str = Field(min_length=2)
    user_id: str = Field(min_length=2)
    role: str = Field(default=ANALYST_ROLE)
    scopes: list[str] = Field(default_factory=lambda: ["metrics:read"])
    expires_in_seconds: int = Field(default=3600, ge=300, le=86_400)


class TokenIssueResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_seconds: int


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


def require_admin(principal: TenantPrincipal) -> None:
    if not principal.is_platform_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="platform admin role required")


def require_scope(principal: TenantPrincipal, tenant_id: str) -> None:
    if not principal.can_access(tenant_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"user {principal.user_id} cannot access tenant {tenant_id}",
        )


@app.on_event("startup")
async def startup() -> None:
    await postgres.connect()


@app.on_event("shutdown")
async def shutdown() -> None:
    await postgres.close()


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"service": SERVICE_NAME, "status": "ok", "environment": settings.environment}


@app.get("/metrics")
async def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/system/status")
async def system_status() -> dict[str, Any]:
    return {"service": SERVICE_NAME, "status": "ok", "request_metrics": request_metrics.snapshot()}


@app.post("/auth/token", response_model=TokenIssueResponse)
async def issue_token(
    request: TokenIssueRequest,
    principal: TenantPrincipal = Depends(get_principal),
) -> TokenIssueResponse:
    if request.role == "platform_admin":
        require_admin(principal)
    elif not principal.can_access(request.tenant_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"user {principal.user_id} cannot issue token for tenant {request.tenant_id}",
        )
    try:
        token_principal = TenantPrincipal(
            user_id=request.user_id,
            tenant_id=request.tenant_id,
            role=request.role,
            scopes=tuple(request.scopes),
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return TokenIssueResponse(
        access_token=create_access_token(
            token_principal,
            expires_in_seconds=request.expires_in_seconds,
        ),
        expires_in_seconds=request.expires_in_seconds,
    )


@app.get("/tenants")
async def tenants(principal: TenantPrincipal = Depends(get_principal)) -> list[dict[str, Any]]:
    require_admin(principal)
    return await repository.list_tenants()


@app.put("/tenants/{tenant_id}")
async def upsert_tenant(
    tenant_id: str,
    request: TenantUpsertRequest,
    principal: TenantPrincipal = Depends(get_principal),
) -> dict[str, Any]:
    require_admin(principal)
    if request.tenant_id != tenant_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="tenant_id mismatch")
    payload = request.model_dump()
    payload["config_json"] = json.dumps(payload.pop("config"))
    return await repository.upsert_tenant(payload)


@app.get("/tenants/{tenant_id}/users")
async def users(
    tenant_id: str,
    principal: TenantPrincipal = Depends(get_principal),
) -> list[dict[str, Any]]:
    require_scope(principal, tenant_id)
    return await repository.list_users(tenant_id)


@app.put("/tenants/{tenant_id}/users/{user_id}")
async def upsert_user(
    tenant_id: str,
    user_id: str,
    request: UserUpsertRequest,
    principal: TenantPrincipal = Depends(get_principal),
) -> dict[str, Any]:
    require_admin(principal)
    if request.tenant_id != tenant_id or request.user_id != user_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="path and body identifiers must match")
    return await repository.upsert_user(request.model_dump())


@app.get("/tenants/{tenant_id}/products")
async def products(
    tenant_id: str,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    principal: TenantPrincipal = Depends(get_principal),
) -> list[dict[str, Any]]:
    require_scope(principal, tenant_id)
    return await repository.list_products(tenant_id, limit, offset)
