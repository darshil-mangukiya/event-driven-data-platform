from __future__ import annotations

import json
from datetime import datetime, timezone

from reliability.evidence import write_evidence
from reliability.models import ScenarioResult, StepResult


def _result(steps: list[StepResult]) -> ScenarioResult:
    result = ScenarioResult(
        scenario_id="unit-test-scenario",
        title="Unit Test Scenario",
        component="test-component",
        expected_behavior="expected",
        detection_method="detection",
        impact="impact",
        root_cause="root cause",
        recovery="recovery",
        corrective_action="corrective",
        preventive_control="preventive",
        steps=steps,
    )
    result.ended_at = datetime.now(timezone.utc)
    return result


def test_overall_status_passed_when_no_failures():
    result = _result([StepResult("s1", "verified", "ok"), StepResult("s2", "simulated", "ok")])
    assert result.overall_status == "passed"


def test_overall_status_failed_if_any_step_failed():
    result = _result([StepResult("s1", "verified", "ok"), StepResult("s2", "failed", "boom")])
    assert result.overall_status == "failed"


def test_overall_status_not_run_when_all_steps_skipped():
    result = _result([StepResult("s1", "not_run", "no infra"), StepResult("s2", "not_run", "no infra")])
    assert result.overall_status == "not_run"


def test_validation_status_labels():
    assert "VERIFIED" in _result([StepResult("s1", "verified", "ok")]).validation_status
    assert "SIMULATED" in _result([StepResult("s1", "simulated", "ok")]).validation_status
    assert "PARTIALLY VERIFIED" in _result(
        [StepResult("s1", "verified", "ok"), StepResult("s2", "simulated", "ok")]
    ).validation_status
    assert "FAILED" in _result([StepResult("s1", "failed", "boom")]).validation_status
    assert "NOT RUN" in _result([StepResult("s1", "not_run", "skip")]).validation_status


def test_write_evidence_creates_all_required_files(tmp_path):
    result = _result([StepResult("s1", "verified", "ok", evidence={"key": "value"})])
    out_dir = write_evidence(result, artifacts_root=tmp_path)

    assert out_dir == tmp_path / result.run_id
    for filename in ("scenario.json", "incident_report.md", "metrics.json", "validation.json", "remediation.md"):
        assert (out_dir / filename).exists(), f"missing {filename}"

    scenario = json.loads((out_dir / "scenario.json").read_text())
    assert scenario["scenario_id"] == "unit-test-scenario"
    assert scenario["overall_status"] == "passed"

    metrics = json.loads((out_dir / "metrics.json").read_text())
    assert metrics["s1"] == {"key": "value"}

    incident_report = (out_dir / "incident_report.md").read_text()
    assert "local reliability exercise / failure simulation" in incident_report.lower()
    assert "Incident ID" in incident_report
    assert result.run_id in incident_report

    remediation = (out_dir / "remediation.md").read_text()
    assert "Root-Cause Summary" in remediation
    assert "Preventive Control" in remediation


def test_write_evidence_never_claims_production_incident(tmp_path):
    result = _result([StepResult("s1", "verified", "ok")])
    out_dir = write_evidence(result, artifacts_root=tmp_path)
    report = " ".join((out_dir / "incident_report.md").read_text().lower().split())
    assert "not a real production incident" in report
