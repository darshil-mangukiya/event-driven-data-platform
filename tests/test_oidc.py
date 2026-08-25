"""OIDC/JWKS verification and authentication-routing tests."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

import jwt
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "services" / "shared"))

from platform_shared.oidc import (  # noqa: E402
    JWKSClient,
    JWKSError,
    oidc_claims_to_principal_kwargs,
    verify_oidc_token,
)


def _generate_rsa_jwks_and_signer():
    """A real, locally-generated RSA keypair — used for the fast unit
    tests below so they don't depend on a running Keycloak, while still
    exercising genuine RS256 signing/verification (not mocked crypto).
    """
    from cryptography.hazmat.primitives.asymmetric import rsa
    from jwt.algorithms import RSAAlgorithm

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    kid = "test-key-1"
    jwk_dict = RSAAlgorithm.to_jwk(public_key, as_dict=True)
    jwk_dict["kid"] = kid
    jwk_dict["use"] = "sig"
    jwk_dict["alg"] = "RS256"
    jwks_document = {"keys": [jwk_dict]}

    def sign(claims: dict, *, signing_kid: str = kid) -> str:
        return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": signing_kid})

    return jwks_document, sign


def test_jwks_client_caches_keys_by_kid() -> None:
    jwks_document, sign = _generate_rsa_jwks_and_signer()
    client = JWKSClient(jwks_url="http://fake/jwks")
    with patch.object(client, "_fetch", return_value={k["kid"]: k for k in jwks_document["keys"]}) as mock_fetch:
        client.get_key("test-key-1")
        client.get_key("test-key-1")
    assert mock_fetch.call_count == 1, "second lookup of an already-cached kid should not re-fetch"


def test_jwks_client_refreshes_once_on_an_unknown_kid() -> None:
    client = JWKSClient(jwks_url="http://fake/jwks")
    call_count = {"n": 0}

    def fake_fetch():
        call_count["n"] += 1
        return {"known-kid": {"kid": "known-kid"}}

    with patch.object(client, "_fetch", side_effect=fake_fetch):
        with pytest.raises(JWKSError, match="no key with kid"):
            client.get_key("never-existed")
    assert call_count["n"] == 2, "should refresh once on cache miss before giving up (rotation handling)"


def test_verify_oidc_token_accepts_a_validly_signed_token_with_tenant_claim() -> None:
    jwks_document, sign = _generate_rsa_jwks_and_signer()
    client = JWKSClient(jwks_url="http://fake/jwks")
    with patch.object(client, "_fetch", return_value={k["kid"]: k for k in jwks_document["keys"]}):
        token = sign(
            {
                "iss": "https://idp.example/realm",
                "sub": "u1",
                "tenant_id": "tenant_enterprise",
                "aud": "my-client",
                "exp": int(time.time()) + 60,
            }
        )
        claims = verify_oidc_token(token, jwks_client=client, issuer="https://idp.example/realm", audience="my-client")
    assert claims["sub"] == "u1"
    assert oidc_claims_to_principal_kwargs(claims)["tenant_id"] == "tenant_enterprise"


def test_verify_oidc_token_rejects_unknown_kid() -> None:
    jwks_document, sign = _generate_rsa_jwks_and_signer()
    client = JWKSClient(jwks_url="http://fake/jwks")
    with patch.object(client, "_fetch", return_value={k["kid"]: k for k in jwks_document["keys"]}):
        token = sign(
            {"iss": "https://idp.example/realm", "sub": "u1", "exp": int(time.time()) + 60},
            signing_kid="unknown-key",
        )
        with pytest.raises(JWKSError, match="no key with kid"):
            verify_oidc_token(token, jwks_client=client, issuer="https://idp.example/realm")


def test_verify_oidc_token_rejects_wrong_issuer() -> None:
    jwks_document, sign = _generate_rsa_jwks_and_signer()
    client = JWKSClient(jwks_url="http://fake/jwks")
    with patch.object(client, "_fetch", return_value={k["kid"]: k for k in jwks_document["keys"]}):
        token = sign({"iss": "https://idp.example/realm", "sub": "u1", "exp": int(time.time()) + 60})
        with pytest.raises(jwt.InvalidIssuerError):
            verify_oidc_token(token, jwks_client=client, issuer="https://different-idp.example/realm")


def test_verify_oidc_token_rejects_wrong_audience() -> None:
    """A token with a valid signature, valid issuer, and valid (not yet
    expired) claims must still be rejected if it was issued for a
    different audience — verified live against a real Keycloak instance
    (evidence/validation/oidc-verification.md), and here against the
    same real `verify_oidc_token` code path, offline/deterministic, so
    this specific rejection has its own repository-level regression test
    rather than relying solely on the live integration test below.
    """
    jwks_document, sign = _generate_rsa_jwks_and_signer()
    client = JWKSClient(jwks_url="http://fake/jwks")
    with patch.object(client, "_fetch", return_value={k["kid"]: k for k in jwks_document["keys"]}):
        token = sign(
            {
                "iss": "https://idp.example/realm",
                "sub": "u1",
                "aud": "some-other-client",
                "exp": int(time.time()) + 60,
            }
        )
        with pytest.raises(jwt.InvalidAudienceError):
            verify_oidc_token(token, jwks_client=client, issuer="https://idp.example/realm", audience="my-client")


def test_verify_oidc_token_rejects_expired_token() -> None:
    jwks_document, sign = _generate_rsa_jwks_and_signer()
    client = JWKSClient(jwks_url="http://fake/jwks")
    with patch.object(client, "_fetch", return_value={k["kid"]: k for k in jwks_document["keys"]}):
        token = sign({"iss": "https://idp.example/realm", "sub": "u1", "exp": int(time.time()) - 60})
        with pytest.raises(jwt.ExpiredSignatureError):
            verify_oidc_token(token, jwks_client=client, issuer="https://idp.example/realm")


def test_verify_oidc_token_rejects_tampered_signature() -> None:
    jwks_document, sign = _generate_rsa_jwks_and_signer()
    client = JWKSClient(jwks_url="http://fake/jwks")
    with patch.object(client, "_fetch", return_value={k["kid"]: k for k in jwks_document["keys"]}):
        token = sign({"iss": "https://idp.example/realm", "sub": "u1", "exp": int(time.time()) + 60})
        parts = token.split(".")
        tampered = ".".join([parts[0], parts[1], parts[2][:-2] + ("aa" if parts[2][-2:] != "aa" else "bb")])
        with pytest.raises((jwt.InvalidSignatureError, jwt.DecodeError)):
            verify_oidc_token(tampered, jwks_client=client, issuer="https://idp.example/realm")


def test_verify_oidc_token_rejects_non_rs256_algorithm() -> None:
    jwks_document, _ = _generate_rsa_jwks_and_signer()
    client = JWKSClient(jwks_url="http://fake/jwks")
    token = jwt.encode(
        {"iss": "https://idp.example/realm", "sub": "u1", "exp": int(time.time()) + 60},
        "untrusted-hmac-secret",
        algorithm="HS256",
        headers={"kid": "test-key-1"},
    )
    with patch.object(client, "_fetch", return_value={k["kid"]: k for k in jwks_document["keys"]}):
        with pytest.raises(jwt.InvalidAlgorithmError):
            verify_oidc_token(token, jwks_client=client, issuer="https://idp.example/realm")


def test_oidc_claims_to_principal_kwargs_uses_configured_claim_names() -> None:
    claims = {"sub": "u1", "preferred_username": "alice", "org_id": "tenant_enterprise", "user_role": "tenant_admin", "scope": "email profile"}
    kwargs = oidc_claims_to_principal_kwargs(claims, tenant_claim="org_id", role_claim="user_role")
    assert kwargs == {"user_id": "alice", "tenant_id": "tenant_enterprise", "role": "tenant_admin", "scopes": ("email", "profile")}


@pytest.mark.parametrize(
    "claims",
    [
        {"sub": "u1"},
        {"sub": "u1", "tenant_id": ""},
        {"sub": "u1", "tenant_id": "   "},
        {"sub": "u1", "tenant_id": None},
        {"sub": "u1", "tenant_id": ["tenant_demo"]},
        {"sub": "u1", "tenant_id": 42},
    ],
)
def test_oidc_claims_to_principal_kwargs_rejects_invalid_tenant_claim(claims: dict) -> None:
    with pytest.raises(ValueError, match="tenant claim"):
        oidc_claims_to_principal_kwargs(claims)


def test_principal_from_authorization_routes_a_kid_bearing_token_to_oidc(monkeypatch) -> None:
    """Regression: a token with a `kid` header must be routed to OIDC
    verification, not the local HS256 scaffold, whenever OIDC is
    configured — this is the actual dispatch logic
    principal_from_authorization uses.
    """
    from platform_shared.auth import TenantPrincipal, principal_from_authorization

    monkeypatch.setenv("OIDC_JWKS_URL", "http://fake/jwks")
    monkeypatch.setenv("OIDC_ISSUER", "https://idp.example/realm")

    fake_principal = TenantPrincipal(user_id="oidc-user", tenant_id="tenant_demo", role="analyst")
    jwks_document, sign = _generate_rsa_jwks_and_signer()
    token = sign({"iss": "https://idp.example/realm", "sub": "u1", "exp": int(time.time()) + 60})

    with patch("platform_shared.auth.principal_from_oidc_token", return_value=fake_principal) as mock_oidc:
        result = principal_from_authorization(authorization=f"Bearer {token}", tenant_id=None, user_id=None, role=None)
    assert result == fake_principal
    mock_oidc.assert_called_once_with(token)


def test_principal_from_authorization_uses_local_scaffold_when_oidc_not_configured(monkeypatch) -> None:
    from platform_shared.auth import (
        ANALYST_ROLE,
        TenantPrincipal,
        create_access_token,
        principal_from_authorization,
    )

    monkeypatch.delenv("OIDC_JWKS_URL", raising=False)
    monkeypatch.delenv("OIDC_ISSUER", raising=False)
    monkeypatch.setenv("JWT_SECRET", "test-secret")

    token = create_access_token(TenantPrincipal(user_id="u1", tenant_id="tenant_demo", role=ANALYST_ROLE))
    principal = principal_from_authorization(authorization=f"Bearer {token}", tenant_id=None, user_id=None, role=None)
    assert principal.user_id == "u1"


# Optional Keycloak integration.


@pytest.mark.integration
def test_live_keycloak_token_verification() -> None:
    import httpx

    from reliability.injectors.reachability import tcp_reachable

    keycloak_url = os.getenv("KEYCLOAK_TEST_URL", "http://localhost:8180")
    if not tcp_reachable("localhost", 8180, timeout=1.0):
        pytest.skip("no Keycloak instance reachable at localhost:8180")

    token_response = httpx.post(
        f"{keycloak_url}/realms/master/protocol/openid-connect/token",
        data={"client_id": "admin-cli", "username": "admin", "password": "admin", "grant_type": "password"},
        timeout=5,
    )
    token_response.raise_for_status()
    access_token = token_response.json()["access_token"]

    client = JWKSClient(jwks_url=f"{keycloak_url}/realms/master/protocol/openid-connect/certs")
    claims = verify_oidc_token(access_token, jwks_client=client, issuer=f"{keycloak_url}/realms/master")
    assert claims["preferred_username"] == "admin"

    # Confirm issuer rejection against the same token.
    with pytest.raises(jwt.InvalidIssuerError):
        verify_oidc_token(access_token, jwks_client=client, issuer="http://not-the-real-issuer/realm")
