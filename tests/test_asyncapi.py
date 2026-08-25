"""Tests for the corresponding verification AsyncAPI event-architecture documentation
(contracts/asyncapi.yml, scripts/generate_asyncapi.py, scripts/validate_asyncapi.py).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "services" / "shared"))


def test_asyncapi_yaml_is_valid_and_matches_real_topics() -> None:
    yaml = pytest.importorskip("yaml")
    from platform_shared.kafka import TOPIC_DEFINITIONS

    document = yaml.safe_load((PROJECT_ROOT / "contracts" / "asyncapi.yml").read_text())
    assert document["asyncapi"] == "3.0.0"
    assert set(document["channels"].keys()) == set(TOPIC_DEFINITIONS.keys())


def test_asyncapi_cross_reference_validator_passes_against_the_real_repo() -> None:
    import importlib.util

    spec = importlib.util.spec_from_file_location("validate_asyncapi", PROJECT_ROOT / "scripts" / "validate_asyncapi.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    errors = module.validate()
    assert errors == [], errors


def test_asyncapi_cross_reference_validator_catches_an_unknown_channel(tmp_path, monkeypatch) -> None:
    import importlib.util

    spec = importlib.util.spec_from_file_location("validate_asyncapi2", PROJECT_ROOT / "scripts" / "validate_asyncapi.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    monkeypatch.setattr(module, "CONTRACTS_ROOT", tmp_path)
    (tmp_path / "asyncapi.yml").write_text(
        "asyncapi: '3.0.0'\ninfo: {}\nchannels:\n  not.a.real.topic:\n    address: not.a.real.topic\n"
        "operations: {}\ncomponents:\n  messages: {}\n"
    )
    errors = module.validate()
    assert any("does not correspond to any topic" in e for e in errors)


def test_generate_asyncapi_produces_every_real_topic_as_a_channel() -> None:
    import importlib.util

    spec = importlib.util.spec_from_file_location("generate_asyncapi", PROJECT_ROOT / "scripts" / "generate_asyncapi.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    from platform_shared.kafka import TOPIC_DEFINITIONS

    document = module.build_asyncapi_document()
    assert set(document["channels"].keys()) == set(TOPIC_DEFINITIONS.keys())
    assert document["operations"], "expected at least one publish/subscribe operation"
