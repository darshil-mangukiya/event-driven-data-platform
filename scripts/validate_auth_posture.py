"""Report the platform's current authentication posture.

The code-level default (AUTH_MODE unset) is "strict" — this only warns when
AUTH_MODE=dev_compat is explicitly set, which this project's own
.env.example does, deliberately, for local curl/demo convenience (see
services/shared/platform_shared/auth.py::_auth_mode and docs/security.md).
That is not a bug; the point of this script is to make the choice *visible*
rather than silent. `--require-strict` turns it into a hard gate for a
deployment context where header-based tenant/role spoofing must not be
reachable at all.

Usage:
    python scripts/validate_auth_posture.py [--pretty] [--require-strict]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "services" / "shared"))


def check_auth_mode() -> dict[str, object]:
    from platform_shared.auth import _auth_mode

    mode = _auth_mode()
    return {
        "name": "auth_mode",
        "value": mode,
        "status": "ok" if mode == "strict" else "warn",
        "detail": (
            "AUTH_MODE=strict (the secure default) — a signed JWT is required on every request."
            if mode == "strict"
            else "AUTH_MODE=dev_compat — unsigned X-Tenant-Id/X-User-Id/X-User-Role headers are "
            "trusted when no Authorization: Bearer token is present. This is an explicit, "
            "documented opt-in (see .env.example) for local curl/demo convenience only — "
            "never use it anywhere reachable by anyone other than you."
        ),
    }


def check_jwt_secret_is_not_default() -> dict[str, object]:
    secret = os.getenv("JWT_SECRET", "local-development-secret-change-me")
    is_default = secret == "local-development-secret-change-me"
    return {
        "name": "jwt_secret",
        "value": "default" if is_default else "overridden",
        "status": "warn" if is_default else "ok",
        "detail": (
            "JWT_SECRET is still the checked-in placeholder default — fine for local dev, "
            "must be overridden before this service is reachable by anyone else."
            if is_default
            else "JWT_SECRET has been overridden from the placeholder default."
        ),
    }


def run_checks() -> dict[str, object]:
    checks = [check_auth_mode(), check_jwt_secret_is_not_default()]
    return {
        "status": "warn" if any(c["status"] == "warn" for c in checks) else "ok",
        "checks": checks,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Report the platform's authentication posture.")
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument(
        "--require-strict",
        action="store_true",
        help="Exit non-zero unless AUTH_MODE=strict (for a deployment gate, not local dev).",
    )
    args = parser.parse_args()

    report = run_checks()
    print(json.dumps(report, indent=2 if args.pretty else None, sort_keys=True))

    if args.require_strict:
        auth_check = next(c for c in report["checks"] if c["name"] == "auth_mode")
        if auth_check["value"] != "strict":
            print("AUTH_MODE must be 'strict' (--require-strict was passed)", file=sys.stderr)
            raise SystemExit(1)


if __name__ == "__main__":
    main()
