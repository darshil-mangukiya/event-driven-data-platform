"""OIDC/JWKS token verification for the strict authentication path.

The local development path uses HS256. Configured OIDC issuers use RS256 and
the provider's public JWKS, with issuer, expiration, and optional audience
validation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any


class JWKSError(Exception):
    """Raised for any JWKS fetch/parse/key-lookup failure."""


@dataclass
class JWKSClient:
    """Fetch and cache a provider's JSON Web Key Set by ``kid``."""

    jwks_url: str
    cache_ttl_seconds: float = 300.0
    _keys_by_kid: dict[str, Any] | None = None
    _fetched_at: float = 0.0

    def _fetch(self) -> dict[str, Any]:
        import httpx

        response = httpx.get(self.jwks_url, timeout=5)
        response.raise_for_status()
        body = response.json()
        keys = {key["kid"]: key for key in body.get("keys", []) if "kid" in key}
        if not keys:
            raise JWKSError(f"no keys with a 'kid' found at {self.jwks_url}")
        return keys

    def refresh(self) -> None:
        self._keys_by_kid = self._fetch()
        self._fetched_at = time.monotonic()

    def _cache_is_stale(self) -> bool:
        return self._keys_by_kid is None or (time.monotonic() - self._fetched_at) > self.cache_ttl_seconds

    def get_key(self, kid: str) -> dict[str, Any]:
        if self._cache_is_stale():
            self.refresh()
        assert self._keys_by_kid is not None
        if kid not in self._keys_by_kid:
            # Refresh once so a rotated signing key can be discovered.
            self.refresh()
        if kid not in self._keys_by_kid:
            raise JWKSError(f"no key with kid={kid!r} found at {self.jwks_url} (checked, refreshed, checked again)")
        return self._keys_by_kid[kid]


def verify_oidc_token(
    token: str,
    *,
    jwks_client: JWKSClient,
    issuer: str,
    audience: str | None = None,
) -> dict[str, Any]:
    """Verify an RS256 JWT against a provider's JWKS."""
    import jwt
    from jwt import PyJWK

    unverified_header = jwt.get_unverified_header(token)
    kid = unverified_header.get("kid")
    if not kid:
        raise JWKSError("token header has no 'kid' — cannot select a JWKS key")

    jwk_dict = jwks_client.get_key(kid)
    signing_key = PyJWK.from_dict(jwk_dict).key

    decode_kwargs: dict[str, Any] = {"algorithms": ["RS256"], "issuer": issuer}
    if audience is not None:
        decode_kwargs["audience"] = audience
    else:
        decode_kwargs["options"] = {"verify_aud": False}

    return jwt.decode(token, signing_key, **decode_kwargs)


def oidc_claims_to_principal_kwargs(
    claims: dict[str, Any],
    *,
    tenant_claim: str = "tenant_id",
    role_claim: str = "role",
    default_role: str = "analyst",
) -> dict[str, Any]:
    """Map verified OIDC claims to ``TenantPrincipal`` arguments.

    Tenant identity is authorization-critical, so OIDC principals must carry
    the configured claim as a non-empty string. It is never inferred from a
    shared default or an unsigned request value.
    """
    tenant_id = claims.get(tenant_claim)
    if not isinstance(tenant_id, str) or not tenant_id.strip():
        raise ValueError(f"OIDC tenant claim {tenant_claim!r} must be a non-empty string")

    return {
        "user_id": str(claims.get("preferred_username") or claims.get("sub") or "oidc-user"),
        "tenant_id": tenant_id.strip(),
        "role": str(claims.get(role_claim, default_role)),
        "scopes": tuple(str(s) for s in claims.get("scope", "").split() if s),
    }
