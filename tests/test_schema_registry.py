"""Kafka Schema Registry tests:
services/shared/platform_shared/schema_compatibility.py (the shared
compatibility algorithm, extracted from scripts/check_contract_compatibility.py
without changing its behavior) and the runtime Schema Registry service
(services/schema-registry-service).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "services" / "shared"))

from platform_shared.schema_compatibility import (  # noqa: E402
    check_compatibility,
    compare_backward_compatible,
    compare_forward_compatible,
    compare_full_compatible,
)


def test_compare_backward_compatible_allows_a_new_optional_field() -> None:
    old = {"required": ["id"], "properties": {"id": {"type": "string"}}}
    new = {"required": ["id"], "properties": {"id": {"type": "string"}, "extra": {"type": "string"}}}
    assert compare_backward_compatible(old, new) == []


def test_compare_backward_compatible_rejects_a_new_required_field() -> None:
    old = {"required": ["id"], "properties": {"id": {"type": "string"}}}
    new = {"required": ["id", "extra"], "properties": {"id": {"type": "string"}, "extra": {"type": "string"}}}
    errors = compare_backward_compatible(old, new)
    assert any("extra" in e for e in errors)


def test_compare_backward_compatible_rejects_a_type_change() -> None:
    old = {"required": [], "properties": {"amount": {"type": "string"}}}
    new = {"required": [], "properties": {"amount": {"type": "integer"}}}
    errors = compare_backward_compatible(old, new)
    assert any("type changed" in e for e in errors)


def test_compare_backward_compatible_rejects_removed_required_field() -> None:
    old = {"required": ["id", "name"], "properties": {"id": {"type": "string"}, "name": {"type": "string"}}}
    new = {"required": ["id"], "properties": {"id": {"type": "string"}}}
    errors = compare_backward_compatible(old, new)
    assert any("required fields removed" in e for e in errors)


def test_compare_backward_compatible_rejects_additional_properties_tightening() -> None:
    old = {"additionalProperties": True, "required": [], "properties": {}}
    new = {"additionalProperties": False, "required": [], "properties": {}}
    errors = compare_backward_compatible(old, new)
    assert any("additionalProperties" in e for e in errors)


def test_compare_forward_compatible_is_the_mirror_of_backward() -> None:
    old = {"required": ["id"], "properties": {"id": {"type": "string"}}}
    new = {"required": ["id", "extra"], "properties": {"id": {"type": "string"}, "extra": {"type": "string"}}}
    # BACKWARD(old, new) fails (new required field); FORWARD(old, new)
    # should therefore equal BACKWARD(new, old), which is compatible
    # (removing a required field going old->new mirror is fine).
    assert compare_backward_compatible(old, new) != []
    assert compare_forward_compatible(old, new) == compare_backward_compatible(new, old)


def test_compare_full_compatible_requires_both_directions() -> None:
    old = {"required": ["id"], "properties": {"id": {"type": "string"}}}
    new = {"required": ["id"], "properties": {"id": {"type": "string"}, "extra": {"type": "string"}}}
    # Adding an optional field: BACKWARD ok. FORWARD(old,new) == BACKWARD(new,old):
    # new->old would need to drop "extra", which is a property removal -> incompatible.
    assert compare_backward_compatible(old, new) == []
    assert compare_full_compatible(old, new) != []


def test_check_compatibility_dispatches_by_mode() -> None:
    old = {"required": ["id"], "properties": {"id": {"type": "string"}}}
    new = {"required": ["id"], "properties": {"id": {"type": "string"}, "extra": {"type": "string"}}}
    assert check_compatibility(old, new, "BACKWARD") == []
    assert check_compatibility(old, new, "backward") == []  # case-insensitive


def test_check_compatibility_rejects_an_unknown_mode() -> None:
    with pytest.raises(ValueError, match="compatibility mode"):
        check_compatibility({}, {}, "SIDEWAYS")


def test_check_contract_compatibility_script_still_passes_with_the_shared_module() -> None:
    """Regression: refactoring compare_backward_compatible into the shared
    module must not change scripts/check_contract_compatibility.py's
    behavior against the real checked-in order v1->v2 contract fixture.
    """
    spec = importlib.util.spec_from_file_location(
        "check_contract_compatibility", PROJECT_ROOT / "scripts" / "check_contract_compatibility.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    errors = module.validate_cases(PROJECT_ROOT / "contracts" / "compatibility_tests" / "order_v1_to_v2.json")
    assert errors == []


def test_deterministic_fixture_cases_file_classifies_all_four_scenarios_correctly() -> None:
    spec = importlib.util.spec_from_file_location(
        "check_contract_compatibility", PROJECT_ROOT / "scripts" / "check_contract_compatibility.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name + "_2"] = module
    spec.loader.exec_module(module)

    errors = module.validate_cases(
        PROJECT_ROOT / "contracts" / "compatibility_tests" / "deterministic_fixture_cases.json"
    )
    assert errors == [], errors


# ---------------------------------------------------------------------------
# Live integration: the running schema-registry-service itself
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_live_schema_registry_bootstrap_and_fixtures() -> None:
    """Regression for a runtime defect found live during verification:
    re-registering an already-registered, byte-identical schema created a
    redundant new version every time instead of being a no-op — found by
    running scripts/validate_schema_registry.py --bootstrap twice in a
    row against a real running registry and seeing the version number
    climb. Fixed in services/schema-registry-service/app/main.py
    (register_schema now checks the candidate against the current latest
    and returns it unchanged, HTTP 200, rather than inserting HTTP 201).

    The subject name is randomized per run (not a fixed literal) because
    the registry persists to PostgreSQL across test sessions (found live,
    A fixed subject name made this test
    fail on any run after the first against a long-lived Docker volume,
    since the subject was already registered — the registry was correctly
    being idempotent, the test's fixture data wasn't session-isolated).
    """
    import uuid

    import httpx

    from reliability.injectors.reachability import tcp_reachable

    registry_url = "http://localhost:8010"
    if not tcp_reachable("localhost", 8010, timeout=1.0):
        pytest.skip("schema-registry-service not reachable at localhost:8010")

    with httpx.Client(timeout=5) as client:
        health = client.get(f"{registry_url}/health")
        assert health.status_code == 200

        schema = {"type": "object", "required": ["id"], "properties": {"id": {"type": "string"}}}
        subject = f"test-idempotent-registration-subject-{uuid.uuid4().hex[:12]}"

        first = client.post(f"{registry_url}/subjects/{subject}/versions", json={"schema_json": schema})
        assert first.status_code == 201
        first_version = first.json()["version"]

        second = client.post(f"{registry_url}/subjects/{subject}/versions", json={"schema_json": schema})
        assert second.status_code == 200, "re-registering an identical schema must be a no-op (200), not a new version (201)"
        assert second.json()["version"] == first_version, "must return the same version, not increment"

        versions = client.get(f"{registry_url}/subjects/{subject}/versions").json()
        assert versions == [first_version], f"expected exactly one version registered, got {versions}"


def test_all_four_fixture_pairs_exist_on_disk() -> None:
    fixtures_dir = PROJECT_ROOT / "contracts" / "compatibility_tests" / "fixtures"
    for filename in (
        "compatible_optional_field_add_old.schema.json",
        "compatible_optional_field_add_new.schema.json",
        "compatible_safe_change_old.schema.json",
        "compatible_safe_change_new.schema.json",
        "breaking_required_field_old.schema.json",
        "breaking_required_field_new.schema.json",
        "breaking_type_change_old.schema.json",
        "breaking_type_change_new.schema.json",
    ):
        assert (fixtures_dir / filename).exists(), filename
