"""Tests for the authentication design in
services/shared/platform_shared/auth.py and scripts/validate_auth_posture.py.

History: verification found that `principal_from_authorization()` — called by
every FastAPI service's `get_principal()` dependency (ingestion, analytics,
metadata) — silently trusted unsigned `X-Tenant-Id`/`X-User-Id`/
`X-User-Role` request headers whenever no `Authorization: Bearer` header was
present, with the permissive behavior as the *default*. Live-verified at the
time: a request with zero authentication and `X-User-Role: platform_admin`
returned 200 against a real running service.

With `AUTH_MODE` unset entirely, the platform
requires a signed JWT on every request ("strict", the default). The
permissive alternative still exists — local curl/demo workflows need
*something* that doesn't require minting a JWT per request — but it is
renamed to `AUTH_MODE=dev_compat` (an explicit, deliberate opt-in that this
project's own `.env.example` sets, visibly, with an explanatory comment) and
is no longer what a bare, unconfigured process falls back to. `"header"` is
still accepted as a deprecated alias for `dev_compat` so nothing with an old
value lying around silently changes behavior, but it is never the
recommended or documented spelling anymore.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "services" / "shared"))

from platform_shared.auth import (  # noqa: E402
    ANALYST_ROLE,
    AUTH_MODE_DEV_COMPAT,
    AUTH_MODE_STRICT,
    PLATFORM_ADMIN_ROLE,
    TenantPrincipal,
    _auth_mode,
    create_access_token,
    principal_from_authorization,
)


def test_auth_mode_defaults_to_strict_when_unset() -> None:
    """The security-critical property: a bare process with no AUTH_MODE set
    at all — e.g. this library reused somewhere without this project's
    curated .env — must default to the secure behavior, not the permissive
    one.
    """
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("AUTH_MODE", None)
        assert _auth_mode() == AUTH_MODE_STRICT


def test_auth_mode_accepts_dev_compat_and_the_deprecated_header_alias() -> None:
    with patch.dict(os.environ, {"AUTH_MODE": "dev_compat"}):
        assert _auth_mode() == AUTH_MODE_DEV_COMPAT
    with patch.dict(os.environ, {"AUTH_MODE": "header"}):
        assert _auth_mode() == AUTH_MODE_DEV_COMPAT  # deprecated alias, same behavior


def test_auth_mode_rejects_unknown_values() -> None:
    with patch.dict(os.environ, {"AUTH_MODE": "yolo"}):
        with pytest.raises(ValueError, match="AUTH_MODE"):
            _auth_mode()


# ---------------------------------------------------------------------------
# The release-blocking property: unsigned headers cannot grant privileges
# in the default/secure configuration. Tested against two distinct tenants.
# ---------------------------------------------------------------------------


def test_default_secure_mode_rejects_requests_with_no_authorization_header() -> None:
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("AUTH_MODE", None)
        with pytest.raises(ValueError, match="Authorization: Bearer"):
            principal_from_authorization(authorization=None, tenant_id="tenant_demo", user_id="dev", role="analyst")


@pytest.mark.parametrize("spoofed_tenant_id", ["tenant_demo", "tenant_enterprise"])
def test_default_secure_mode_blocks_spoofed_admin_headers_for_any_tenant(spoofed_tenant_id: str) -> None:
    """The concrete attack the original finding was built on: no
    Authorization header, but X-User-Role set to platform_admin and
    X-Tenant-Id set to a tenant the caller has no legitimate relationship
    to. Verified against two distinct tenant ids — this must be rejected
    outright for either, beyond the one tenant used in the original
    finding.
    """
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("AUTH_MODE", None)
        with pytest.raises(ValueError):
            principal_from_authorization(
                authorization=None,
                tenant_id=spoofed_tenant_id,
                user_id="attacker",
                role=PLATFORM_ADMIN_ROLE,
            )


def test_default_secure_mode_accepts_a_valid_signed_jwt() -> None:
    with patch.dict(os.environ, {"JWT_SECRET": "test-secret-for-this-test-only"}, clear=False):
        os.environ.pop("AUTH_MODE", None)
        token = create_access_token(TenantPrincipal(user_id="u1", tenant_id="tenant_demo", role=ANALYST_ROLE))
        principal = principal_from_authorization(
            authorization=f"Bearer {token}", tenant_id=None, user_id=None, role=None
        )
    assert principal.tenant_id == "tenant_demo"
    assert principal.user_id == "u1"


def test_default_secure_mode_jwt_for_one_tenant_cannot_access_another() -> None:
    """Cross-tenant isolation, exercised at the principal level: a
    legitimately-issued JWT for tenant_demo must not satisfy
    can_access()/require_tenant_access() for tenant_enterprise.
    """
    with patch.dict(os.environ, {"JWT_SECRET": "test-secret-for-this-test-only"}, clear=False):
        os.environ.pop("AUTH_MODE", None)
        token = create_access_token(TenantPrincipal(user_id="u1", tenant_id="tenant_demo", role=ANALYST_ROLE))
        principal = principal_from_authorization(
            authorization=f"Bearer {token}", tenant_id=None, user_id=None, role=None
        )
    assert principal.can_access("tenant_demo") is True
    assert principal.can_access("tenant_enterprise") is False


# ---------------------------------------------------------------------------
# dev_compat: explicit opt-in only, never accidental
# ---------------------------------------------------------------------------


def test_dev_compat_mode_must_be_explicitly_set_not_implied_by_anything_else() -> None:
    """Nothing except the literal AUTH_MODE=dev_compat (or its deprecated
    'header' alias) value enables the permissive fallback — not an empty
    string, not a falsy-looking value, not any other unrecognized value
    (those raise, per test_auth_mode_rejects_unknown_values above).
    """
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("AUTH_MODE", None)
        assert _auth_mode() != AUTH_MODE_DEV_COMPAT


def test_dev_compat_mode_still_trusts_x_headers_when_no_authorization_present() -> None:
    """Preserves the local-dev-convenience behavior for the explicit
    opt-in case — this is what this project's own .env.example enables for
    its zero-JWT local demo workflow (README.md, CONTRIBUTING.md).
    """
    with patch.dict(os.environ, {"AUTH_MODE": "dev_compat"}):
        principal = principal_from_authorization(
            authorization=None, tenant_id="tenant_demo", user_id="dev", role="analyst"
        )
    assert principal.tenant_id == "tenant_demo"
    assert principal.role_key == ANALYST_ROLE


def test_dev_compat_mode_default_cannot_be_escalated_to_admin_by_omission() -> None:
    """Even in the explicit-opt-in permissive mode, sending zero headers at
    all must not produce a platform_admin principal — the unauthenticated
    fallback identity stays a plain analyst on the fallback tenant. Full
    admin impersonation still requires deliberately setting X-User-Role,
    which the default AUTH_MODE=strict is what actually prevents.
    """
    with patch.dict(os.environ, {"AUTH_MODE": "dev_compat"}):
        principal = principal_from_authorization(authorization=None, tenant_id=None, user_id=None, role=None)
    assert principal.role_key != PLATFORM_ADMIN_ROLE


def test_env_example_sets_auth_mode_to_dev_compat_explicitly() -> None:
    """Regression: the checked-in .env.example is what makes dev_compat an
    explicit, visible opt-in rather than a silent one. If this line ever
    disappears or reverts to a bare 'header', the local stack would
    silently start requiring JWTs everywhere (breaking the documented
    zero-setup demo flow) or silently reintroduce the original permissive
    default depending on which way it drifted — either is a signal this
    test should catch, not discover live.
    """
    env_example = (PROJECT_ROOT / ".env.example").read_text()
    assert "AUTH_MODE=dev_compat" in env_example


# ---------------------------------------------------------------------------
# validate_auth_posture.py
# ---------------------------------------------------------------------------


def _load_validate_auth_posture():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "validate_auth_posture", PROJECT_ROOT / "scripts" / "validate_auth_posture.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


validate_auth_posture = _load_validate_auth_posture()


def test_validate_auth_posture_ok_by_default_strict_mode() -> None:
    with patch.dict(os.environ, {"JWT_SECRET": "not-the-default"}, clear=False):
        os.environ.pop("AUTH_MODE", None)
        report = validate_auth_posture.run_checks()
    assert report["status"] == "ok"


def test_validate_auth_posture_warns_on_dev_compat_mode() -> None:
    with patch.dict(os.environ, {"AUTH_MODE": "dev_compat"}):
        report = validate_auth_posture.run_checks()
    assert report["status"] == "warn"
    auth_check = next(c for c in report["checks"] if c["name"] == "auth_mode")
    assert auth_check["status"] == "warn"
    assert auth_check["value"] == "dev_compat"


def test_validate_auth_posture_warns_on_default_jwt_secret() -> None:
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("JWT_SECRET", None)
        check = validate_auth_posture.check_jwt_secret_is_not_default()
    assert check["status"] == "warn"
    assert check["value"] == "default"
