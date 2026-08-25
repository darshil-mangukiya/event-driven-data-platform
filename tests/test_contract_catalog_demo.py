from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.demo_mode import build_demo_plan
from scripts.resilience_probe import load_scenarios, run_probe
from scripts.validate_catalog import validate_catalog
from scripts.validate_event_contracts import validate_contract_registry

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_event_contract_registry_covers_all_event_types() -> None:
    errors = validate_contract_registry(PROJECT_ROOT / "contracts" / "registry.json")

    assert errors == []


def test_data_catalog_references_are_valid() -> None:
    catalog = json.loads((PROJECT_ROOT / "catalog" / "data_catalog.json").read_text())

    assert validate_catalog(catalog) == []


def test_demo_plan_can_skip_stack_and_still_prepare_local_steps() -> None:
    args = argparse.Namespace(
        skip_stack=True,
        timeout_seconds=5,
        synthetic_count=10,
        settle_seconds=1,
    )

    plan = build_demo_plan(args)

    assert [step.name for step in plan] == [
        "wait_for_services",
        "generate_synthetic_events",
        "local_e2e",
        "quality_checks",
        "benchmark_report",
    ]
    assert plan[1].command[-1] == "data/synthetic/demo_events.jsonl"


def test_resilience_scenarios_have_expected_metadata() -> None:
    scenarios = load_scenarios(PROJECT_ROOT / "chaos" / "scenarios.json")
    results = run_probe(scenarios, dry_run=True)

    assert {scenario["name"] for scenario in scenarios} >= {
        "kafka_unavailable",
        "postgres_unavailable",
        "redis_unavailable",
    }
    assert all(result["probe"]["dry_run"] for result in results)
