from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from platform_shared.schemas import EventType, validate_event_payload

DOMAIN_FIXTURE_CASES = {
    "orders": ("order.created", "order.created.valid.json", "order.created.invalid.json"),
    "payments": ("payment.captured", "payment.captured.valid.json", "payment.captured.invalid.json"),
    "users": ("user.activity", "user.activity.valid.json", "user.activity.invalid.json"),
    "products": (
        "product.inventory_changed",
        "product.inventory_changed.valid.json",
        "product.inventory_changed.invalid.json",
    ),
    "system": ("system.health", "system.health.valid.json", "system.health.invalid.json"),
}


def load_registry(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def registry_event_types(registry: dict[str, Any]) -> set[str]:
    return {
        event_type
        for subject in registry["subjects"]
        for event_type in subject.get("event_types", [])
    }


def validate_contract_registry(registry_path: Path) -> list[str]:
    errors: list[str] = []
    registry = load_registry(registry_path)
    root = registry_path.parent

    envelope_path = root / registry["envelope_schema"]
    if not envelope_path.exists():
        errors.append(f"missing envelope schema: {envelope_path}")
    else:
        envelope_schema = json.loads(envelope_path.read_text())
        required = set(envelope_schema.get("required", []))
        expected_required = {
            "event_id",
            "tenant_id",
            "event_type",
            "event_timestamp",
            "source_service",
            "payload_version",
            "payload",
            "trace_id",
            "correlation_id",
            "idempotency_key",
        }
        missing = expected_required - required
        if missing:
            errors.append(f"envelope schema missing required fields: {sorted(missing)}")

    covered = registry_event_types(registry)
    expected = {event_type.value for event_type in EventType}
    if covered != expected:
        errors.append(
            f"registry event type coverage mismatch missing={sorted(expected - covered)} extra={sorted(covered - expected)}"
        )

    seen_subjects: set[str] = set()
    for subject in registry["subjects"]:
        subject_name = subject["subject"]
        if subject_name in seen_subjects:
            errors.append(f"duplicate subject: {subject_name}")
        seen_subjects.add(subject_name)
        payload_path = root / subject["payload_schema"]
        if not payload_path.exists():
            errors.append(f"missing payload schema for {subject_name}: {payload_path}")
            continue
        payload_schema = json.loads(payload_path.read_text())
        if not payload_schema.get("required"):
            errors.append(f"payload schema has no required fields: {payload_path}")
        if subject.get("version", 0) < 1:
            errors.append(f"invalid subject version for {subject_name}")
        if not subject.get("owner"):
            errors.append(f"missing owner for {subject_name}")

    errors.extend(validate_domain_event_fixtures(root / "events"))
    return errors


def validate_domain_event_fixtures(events_root: Path) -> list[str]:
    errors: list[str] = []
    if not events_root.exists():
        errors.append(f"missing domain event contracts folder: {events_root}")
        return errors

    for domain, (event_type, valid_name, invalid_name) in DOMAIN_FIXTURE_CASES.items():
        domain_root = events_root / domain
        schema_files = list(domain_root.glob("*.schema.json"))
        if not schema_files:
            errors.append(f"missing domain schema for {domain}")
        valid_path = domain_root / "fixtures" / valid_name
        invalid_path = domain_root / "fixtures" / invalid_name
        if not valid_path.exists() or not invalid_path.exists():
            errors.append(f"missing valid/invalid fixtures for {domain}")
            continue

        try:
            validate_event_payload(event_type, json.loads(valid_path.read_text()))
        except Exception as exc:
            errors.append(f"valid fixture failed for {domain}: {exc}")

        try:
            validate_event_payload(event_type, json.loads(invalid_path.read_text()))
            errors.append(f"invalid fixture unexpectedly passed for {domain}")
        except Exception:
            pass
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate event contract registry coverage.")
    parser.add_argument("--registry", default="contracts/registry.json")
    args = parser.parse_args()
    errors = validate_contract_registry(Path(args.registry))
    if errors:
        for error in errors:
            print(f"ERROR {error}")
        raise SystemExit(1)
    print("event contract registry ok")


if __name__ == "__main__":
    main()
