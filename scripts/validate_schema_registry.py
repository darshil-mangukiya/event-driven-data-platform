"""Bootstrap and validate the runtime Schema Registry service.

Three things this script does, each independently useful:

1. `--check-health` — confirm the registry service is reachable.
2. `--bootstrap` — register every subject/schema listed in
   `contracts/registry.json` into the running registry (the actual
   "source-controlled schema -> runtime registry" step). Idempotent: a
   subject whose latest registered schema already matches is left alone.
3. `--check-fixtures` — POST each of the 4 deterministic compatibility
   fixtures (contracts/compatibility_tests/fixtures/) to the registry's
   `/compatibility` endpoint and confirm the live service, in addition to
   the offline function) classifies each one correctly. This is what turns
   "the compatibility algorithm is unit-tested" into "the running registry
   enforces it correctly."

Usage:
    python scripts/validate_schema_registry.py --check-health
    python scripts/validate_schema_registry.py --bootstrap
    python scripts/validate_schema_registry.py --check-fixtures
    python scripts/validate_schema_registry.py --all --pretty
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONTRACTS_ROOT = PROJECT_ROOT / "contracts"
DEFAULT_REGISTRY_URL = "http://localhost:8010"

FIXTURE_CASES = [
    ("fixture_compatible_optional_field_add", "compatible_optional_field_add_old.schema.json", "compatible_optional_field_add_new.schema.json", True),
    ("fixture_compatible_safe_change", "compatible_safe_change_old.schema.json", "compatible_safe_change_new.schema.json", True),
    ("fixture_breaking_required_field", "breaking_required_field_old.schema.json", "breaking_required_field_new.schema.json", False),
    ("fixture_breaking_type_change", "breaking_type_change_old.schema.json", "breaking_type_change_new.schema.json", False),
]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def check_health(registry_url: str) -> dict[str, Any]:
    import httpx

    response = httpx.get(f"{registry_url}/health", timeout=5)
    response.raise_for_status()
    return {"name": "registry_health", "status": "ok", "response": response.json()}


def bootstrap(registry_url: str) -> dict[str, Any]:
    import httpx

    manifest = _load_json(CONTRACTS_ROOT / "registry.json")
    results = []
    with httpx.Client(timeout=10) as client:
        for subject_entry in manifest["subjects"]:
            subject = subject_entry["subject"]
            schema_path = CONTRACTS_ROOT / subject_entry["payload_schema"]
            schema_json = _load_json(schema_path)
            response = client.post(
                f"{registry_url}/subjects/{subject}/versions",
                json={"schema_json": schema_json, "registered_by": "scripts/validate_schema_registry.py --bootstrap"},
            )
            if response.status_code == 201:
                results.append({"subject": subject, "status": "registered", "version": response.json()["version"]})
            elif response.status_code == 200:
                results.append(
                    {"subject": subject, "status": "already_registered_identical", "version": response.json()["version"]}
                )
            elif response.status_code == 409:
                # Already registered with an identical (or incompatible —
                # surfaced as an explicit incompatibility) schema.
                results.append({"subject": subject, "status": "already_registered_or_conflict", "detail": response.json()})
            else:
                response.raise_for_status()
    return {"name": "bootstrap", "status": "ok", "subjects": results}


def check_fixtures(registry_url: str) -> dict[str, Any]:
    import httpx

    fixtures_dir = CONTRACTS_ROOT / "compatibility_tests" / "fixtures"
    outcomes = []
    with httpx.Client(timeout=10) as client:
        for name, old_file, new_file, expected_compatible in FIXTURE_CASES:
            subject = f"validation-fixture-{name}"
            old_schema = _load_json(fixtures_dir / old_file)
            new_schema = _load_json(fixtures_dir / new_file)
            # Seed the "old" schema fresh for each fixture subject so the
            # compatibility check below is deterministic regardless of
            # prior runs (fixture subjects are never real event contracts).
            client.post(f"{registry_url}/subjects/{subject}/versions", json={"schema_json": old_schema})
            response = client.post(
                f"{registry_url}/compatibility/subjects/{subject}/versions/latest",
                json={"schema_json": new_schema},
            )
            response.raise_for_status()
            body = response.json()
            passed = body["is_compatible"] == expected_compatible
            outcomes.append(
                {
                    "fixture": name,
                    "expected_compatible": expected_compatible,
                    "actual_compatible": body["is_compatible"],
                    "errors": body["errors"],
                    "passed": passed,
                }
            )
    all_passed = all(o["passed"] for o in outcomes)
    return {"name": "check_fixtures", "status": "ok" if all_passed else "failed", "fixtures": outcomes}


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap and validate the runtime Schema Registry.")
    parser.add_argument("--registry-url", default=DEFAULT_REGISTRY_URL)
    parser.add_argument("--check-health", action="store_true")
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--check-fixtures", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    if not (args.check_health or args.bootstrap or args.check_fixtures or args.all):
        parser.error("pass at least one of --check-health / --bootstrap / --check-fixtures / --all")

    results: dict[str, Any] = {}
    try:
        if args.check_health or args.all:
            results["health"] = check_health(args.registry_url)
        if args.bootstrap or args.all:
            results["bootstrap"] = bootstrap(args.registry_url)
        if args.check_fixtures or args.all:
            results["fixtures"] = check_fixtures(args.registry_url)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2 if args.pretty else None), file=sys.stderr)
        raise SystemExit(1) from exc

    overall_status = "ok" if all(r.get("status") in ("ok",) for r in results.values()) else "failed"
    print(json.dumps({"status": overall_status, **results}, indent=2 if args.pretty else None, sort_keys=True))
    if overall_status != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
