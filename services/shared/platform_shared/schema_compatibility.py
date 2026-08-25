"""JSON Schema backward-compatibility comparison — the single source of
truth used by both the offline governance script
(scripts/check_contract_compatibility.py) and the runtime Schema Registry
service (services/schema-registry-service). Extracted here during the
Kafka Schema Registry implementation so the same compatibility algorithm
that gates checked-in contract changes also gates runtime schema
registration — no second, drifting implementation.

This implements a practical, deliberately conservative subset of
Confluent-style BACKWARD compatibility for JSON Schema `object` schemas:
a new schema is BACKWARD compatible with an old one if data written
against the *old* schema can still be read/validated against the *new*
one. Concretely: no previously-required field may be removed or have its
type changed; no new field may be added as required (it must be
optional, so old producers that don't send it still validate); no
property may be removed; `additionalProperties` may not tighten from
`true` to `false`. This is not a full JSON Schema Draft equivalence
checker (nested object/array schemas, `enum`, `oneOf`, and format
constraints are not recursively compared) — see the module's own
docstring in each caller for the scoped claim.
"""

from __future__ import annotations

from typing import Any


def compare_backward_compatible(old_schema: dict[str, Any], new_schema: dict[str, Any]) -> list[str]:
    """Return a list of compatibility errors; empty list means compatible."""
    errors: list[str] = []
    old_required = set(old_schema.get("required", []))
    new_required = set(new_schema.get("required", []))
    old_properties = old_schema.get("properties", {})
    new_properties = new_schema.get("properties", {})
    if old_schema.get("additionalProperties", True) is True and new_schema.get("additionalProperties", True) is False:
        errors.append("additionalProperties changed from true to false")

    removed_required = old_required - new_required
    if removed_required:
        errors.append(f"required fields removed: {sorted(removed_required)}")

    newly_required = new_required - old_required
    if newly_required:
        errors.append(f"new required fields are not backward compatible: {sorted(newly_required)}")

    removed_properties = set(old_properties) - set(new_properties)
    if removed_properties:
        errors.append(f"properties removed: {sorted(removed_properties)}")

    for name, old_property in old_properties.items():
        if name not in new_properties:
            continue
        old_type = old_property.get("type")
        new_type = new_properties[name].get("type")
        if old_type != new_type:
            errors.append(f"property {name} type changed from {old_type} to {new_type}")
    return errors


def compare_forward_compatible(old_schema: dict[str, Any], new_schema: dict[str, Any]) -> list[str]:
    """FORWARD compatibility: data written against the *new* schema can
    still be read by consumers using the *old* schema. This is the mirror
    of BACKWARD — swap old/new and re-run the same rule set, since "can X
    read Y" is symmetric in the fields we check (required/removed/type).
    """
    return compare_backward_compatible(new_schema, old_schema)


def compare_full_compatible(old_schema: dict[str, Any], new_schema: dict[str, Any]) -> list[str]:
    """FULL compatibility: both BACKWARD and FORWARD must hold."""
    errors = compare_backward_compatible(old_schema, new_schema)
    errors += [f"(forward) {e}" for e in compare_forward_compatible(old_schema, new_schema) if f"(forward) {e}" not in errors]
    return errors


COMPATIBILITY_CHECKERS = {
    "BACKWARD": compare_backward_compatible,
    "FORWARD": compare_forward_compatible,
    "FULL": compare_full_compatible,
}


def check_compatibility(old_schema: dict[str, Any], new_schema: dict[str, Any], mode: str) -> list[str]:
    mode = mode.strip().upper()
    checker = COMPATIBILITY_CHECKERS.get(mode)
    if checker is None:
        raise ValueError(f"unsupported compatibility mode: {mode!r} (expected one of {sorted(COMPATIBILITY_CHECKERS)})")
    return checker(old_schema, new_schema)
