from __future__ import annotations

import json
from pathlib import Path

from scripts.generate_evidence_bundle import generate_bundle
from scripts.lifecycle_retention_plan import build_plan, load_policies
from scripts.platform_preflight import preflight_checks, render_markdown, run_preflight
from scripts.validate_metric_contracts import validate_metric_contracts, validate_metric_fixture

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_metric_contracts_validate_revenue_fixture() -> None:
    contract = json.loads((PROJECT_ROOT / "metrics" / "contracts" / "tenant_daily_metrics.json").read_text())
    fixture = json.loads((PROJECT_ROOT / "api" / "fixtures" / "analytics_revenue_response.json").read_text())
    revenue_contract = {item["metric"]: item for item in contract["contracts"]}["revenue"]

    assert validate_metric_contracts(contract) == []
    assert validate_metric_fixture(revenue_contract, fixture) == []


def test_lifecycle_retention_plan_uses_privacy_catalog() -> None:
    policies = load_policies(PROJECT_ROOT / "governance" / "pii_classification.json")
    plan = build_plan(policies)

    assert plan["status"] == "dry_run"
    assert plan["policy_count"] >= 1
    assert any(step["dataset"] == "api_usage_log" for step in plan["steps"])


def test_preflight_dry_run_lists_release_gates() -> None:
    report = run_preflight(dry_run=True)
    markdown = render_markdown(report)

    assert report["status"] == "passed"
    assert {check.name for check in preflight_checks()} >= {"schema_drift", "metric_contracts"}
    assert "Release Readiness Report" in markdown


def test_evidence_bundle_contains_capability_matrix(tmp_path: Path) -> None:
    bundle = generate_bundle(tmp_path)

    assert bundle["capability_count"] >= 5
    assert (tmp_path / "capability_matrix.json").exists()
    assert (tmp_path / "README.md").exists()
