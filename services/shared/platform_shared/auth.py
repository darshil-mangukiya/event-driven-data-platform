from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

PLATFORM_ADMIN_ROLE = "platform_admin"
TENANT_ADMIN_ROLE = "tenant_admin"
ANALYST_ROLE = "analyst"
VIEWER_ROLE = "viewer"
SERVICE_ACCOUNT_ROLE = "service_account"
LEGACY_ANALYST_ROLE = "tenant_analyst"
ALLOWED_ROLES = {
    PLATFORM_ADMIN_ROLE,
    TENANT_ADMIN_ROLE,
    ANALYST_ROLE,
    VIEWER_ROLE,
    SERVICE_ACCOUNT_ROLE,
    LEGACY_ANALYST_ROLE,
}
ROLE_ALIASES = {LEGACY_ANALYST_ROLE: ANALYST_ROLE}
DEFAULT_JWT_AUDIENCE = "data-platform"
DEFAULT_JWT_ISSUER = "platform.local"
JWT_ALGORITHM = "HS256"


@dataclass(frozen=True)
class TenantPrincipal:
    user_id: str
    tenant_id: str
    role: str = ANALYST_ROLE
    scopes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.role not in ALLOWED_ROLES:
            raise ValueError(f"unsupported role: {self.role}")
        object.__setattr__(self, "scopes", tuple(self.scopes))

    @property
    def role_key(self) -> str:
        return ROLE_ALIASES.get(self.role, self.role)

    @property
    def is_platform_admin(self) -> bool:
        return self.role_key == PLATFORM_ADMIN_ROLE

    @property
    def is_service_account(self) -> bool:
        return self.role_key == SERVICE_ACCOUNT_ROLE

    def can_access(self, tenant_id: str) -> bool:
        return self.is_platform_admin or self.tenant_id == tenant_id

    def has_scope(self, scope: str) -> bool:
        return self.is_platform_admin or scope in self.scopes


def principal_from_headers(
    *,
    tenant_id: str | None,
    user_id: str | None,
    role: str | None,
    scopes: tuple[str, ...] | None = None,
) -> TenantPrincipal:
    return TenantPrincipal(
        user_id=user_id or "local-dev-user",
        tenant_id=tenant_id or "tenant_demo",
        role=role or "tenant_analyst",
        scopes=scopes or (),
    )


AUTH_MODE_STRICT = "strict"
AUTH_MODE_DEV_COMPAT = "dev_compat"
# Retain the former name as a deprecated compatibility alias.
_DEV_COMPAT_ALIASES = {AUTH_MODE_DEV_COMPAT, "header"}


def _auth_mode() -> str:
    """Return strict mode unless local header compatibility is explicit."""
    raw = os.getenv("AUTH_MODE", AUTH_MODE_STRICT).strip().lower()
    if raw in _DEV_COMPAT_ALIASES:
        return AUTH_MODE_DEV_COMPAT
    if raw == AUTH_MODE_STRICT:
        return AUTH_MODE_STRICT
    raise ValueError(f"AUTH_MODE must be 'strict' or 'dev_compat', got {raw!r}")


def _jwt_secret() -> str:
    return os.getenv("JWT_SECRET", "local-development-secret-change-me")


def _jwt_issuer() -> str:
    return os.getenv("JWT_ISSUER", DEFAULT_JWT_ISSUER)


def _jwt_audience() -> str:
    return os.getenv("JWT_AUDIENCE", DEFAULT_JWT_AUDIENCE)


def create_access_token(
    principal: TenantPrincipal,
    *,
    expires_in_seconds: int = 3600,
    extra_claims: dict[str, Any] | None = None,
) -> str:
    import jwt

    now = int(time.time())
    payload: dict[str, Any] = {
        "sub": principal.user_id,
        "tenant_id": principal.tenant_id,
        "role": principal.role,
        "scopes": list(principal.scopes),
        "iss": _jwt_issuer(),
        "aud": _jwt_audience(),
        "iat": now,
        "exp": now + expires_in_seconds,
    }
    if extra_claims:
        payload.update(extra_claims)
    return jwt.encode(payload, _jwt_secret(), algorithm=JWT_ALGORITHM)


def principal_from_token(token: str) -> TenantPrincipal:
    import jwt

    claims = jwt.decode(
        token,
        _jwt_secret(),
        algorithms=[JWT_ALGORITHM],
        audience=_jwt_audience(),
        issuer=_jwt_issuer(),
    )
    scopes = tuple(str(scope) for scope in claims.get("scopes", []))
    return TenantPrincipal(
        user_id=str(claims["sub"]),
        tenant_id=str(claims["tenant_id"]),
        role=str(claims.get("role", "tenant_analyst")),
        scopes=scopes,
    )


def _oidc_jwks_url() -> str | None:
    return os.getenv("OIDC_JWKS_URL") or None


def _oidc_issuer() -> str | None:
    return os.getenv("OIDC_ISSUER") or None


def _oidc_audience() -> str | None:
    return os.getenv("OIDC_AUDIENCE") or None


_OIDC_JWKS_CLIENT_CACHE: dict[str, Any] = {}


def principal_from_oidc_token(token: str) -> TenantPrincipal:
    """Verify an OIDC token with the configured issuer and JWKS."""
    from platform_shared.oidc import JWKSClient, oidc_claims_to_principal_kwargs, verify_oidc_token

    jwks_url = _oidc_jwks_url()
    issuer = _oidc_issuer()
    assert jwks_url and issuer  # caller (principal_from_authorization) already checked

    if jwks_url not in _OIDC_JWKS_CLIENT_CACHE:
        _OIDC_JWKS_CLIENT_CACHE[jwks_url] = JWKSClient(jwks_url=jwks_url)
    jwks_client = _OIDC_JWKS_CLIENT_CACHE[jwks_url]

    claims = verify_oidc_token(token, jwks_client=jwks_client, issuer=issuer, audience=_oidc_audience())
    kwargs = oidc_claims_to_principal_kwargs(claims)
    return TenantPrincipal(**kwargs)


def principal_from_authorization(
    *,
    authorization: str | None,
    tenant_id: str | None,
    user_id: str | None,
    role: str | None,
) -> TenantPrincipal:
    if authorization:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise ValueError("Authorization header must use Bearer token format")
        if _oidc_jwks_url() and _oidc_issuer():
            # OIDC tokens carry a key id; local HS256 tokens do not.
            import jwt as _jwt

            if _jwt.get_unverified_header(token).get("kid"):
                return principal_from_oidc_token(token)
        return principal_from_token(token)
    if _auth_mode() != AUTH_MODE_DEV_COMPAT:
        raise ValueError(
            "Authorization: Bearer <jwt> is required (AUTH_MODE=strict, the default) — "
            "unsigned X-Tenant-Id/X-User-Id/X-User-Role headers are not trusted unless "
            "AUTH_MODE=dev_compat is explicitly set"
        )
    return principal_from_headers(tenant_id=tenant_id, user_id=user_id, role=role)


def require_tenant_access(principal: TenantPrincipal, tenant_id: str) -> None:
    if not principal.can_access(tenant_id):
        raise PermissionError(f"user {principal.user_id} cannot access tenant {tenant_id}")


def require_scope(principal: TenantPrincipal, scope: str) -> None:
    if not principal.has_scope(scope):
        raise PermissionError(f"user {principal.user_id} is missing scope {scope}")
