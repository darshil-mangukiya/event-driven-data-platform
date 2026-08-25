"""Tests for the AI Streaming Incident Copilot.

Deterministic detection remains authoritative; this copilot never detects
anything itself. All tests here use the offline MockIncidentCopilotProvider
— fully deterministic, with no network calls or API key. The tests exercise
the output produced from `reliability/scenarios/db_outage.py` results.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "services" / "shared"))

from ai_incident_copilot.copilot import run_incident_analysis  # noqa: E402
from ai_incident_copilot.evidence import (  # noqa: E402
    EvidenceItem,
    build_evidence_bundle,
    evidence_from_prometheus_alert,
    evidence_from_reconciliation_results,
    evidence_from_reliability_result,
)
from ai_incident_copilot.provider import (  # noqa: E402
    AnthropicIncidentCopilotProvider,
    IncidentCopilotProvider,
    MockIncidentCopilotProvider,
    ProviderUnavailableError,
    get_default_provider,
)
from ai_incident_copilot.runbooks import find_runbook  # noqa: E402
from ai_incident_copilot.schema import IncidentAnalysis, RootCause  # noqa: E402


class _FakeStep:
    def __init__(self, name: str, status: str, detail: str, evidence: dict | None = None) -> None:
        self.name = name
        self.status = status
        self.detail = detail
        self.evidence = evidence or {}


class _FakeScenarioResult:
    def __init__(self, *, scenario_id: str, title: str, component: str, overall_status: str, root_cause: str, steps: list) -> None:
        self.scenario_id = scenario_id
        self.title = title
        self.expected_behavior = f"{title} expected behavior"
        self.component = component
        self.overall_status = overall_status
        self.root_cause = root_cause
        self.steps = steps


def _passing_scenario() -> _FakeScenarioResult:
    return _FakeScenarioResult(
        scenario_id="db-outage",
        title="PostgreSQL Outage",
        component="spark.streaming.sinks.PostgresSink",
        overall_status="passed",
        root_cause="Network partition or connection pool exhaustion.",
        steps=[_FakeStep("write_fails_with_bounded_retries", "verified", "retried and raised as expected", {"elapsed_seconds": 2.1})],
    )


def _failing_scenario() -> _FakeScenarioResult:
    return _FakeScenarioResult(
        scenario_id="poison-event",
        title="Poison Event",
        component="spark.streaming.validation",
        overall_status="failed",
        root_cause="Unsupported payload_version.",
        steps=[
            _FakeStep("run_real_validation_pipeline", "verified", "classified invalid"),
            _FakeStep("publish_poison_event_to_kafka", "failed", "kafka client unavailable"),
            _FakeStep("assert_routed_to_dlq_path", "failed", "could not confirm DLQ routing"),
        ],
    )


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_incident_analysis_requires_human_approval_true() -> None:
    with pytest.raises(ValidationError, match="requires_human_approval"):
        IncidentAnalysis(incident_type="x", severity="SEV-3", affected_component="y", requires_human_approval=False)


def test_incident_analysis_rejects_confidence_out_of_range() -> None:
    with pytest.raises(ValidationError):
        RootCause(cause="x", confidence=1.5)


def test_incident_analysis_rejects_unknown_severity() -> None:
    with pytest.raises(ValidationError):
        IncidentAnalysis(incident_type="x", severity="SEV-99", affected_component="y")  # type: ignore[arg-type]


def test_incident_analysis_missing_required_fields_raises() -> None:
    with pytest.raises(ValidationError):
        IncidentAnalysis()  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Evidence
# ---------------------------------------------------------------------------


def test_evidence_from_reliability_result_includes_summary_and_every_step() -> None:
    items = evidence_from_reliability_result(_failing_scenario())
    assert len(items) == 4  # 1 summary + 3 steps
    assert items[0].id == "reliability:poison-event:summary"
    assert any(i.data.get("status") == "failed" for i in items[1:])


def test_evidence_from_reconciliation_results() -> None:
    items = evidence_from_reconciliation_results([{"check_name": "revenue", "tenant_id": "tenant_demo", "status": "failed", "metric_date": "2026-01-01"}])
    assert len(items) == 1
    assert "revenue" in items[0].summary


def test_evidence_from_prometheus_alert() -> None:
    item = evidence_from_prometheus_alert("RedisUnavailable", {"severity": "warning"}, {"summary": "Redis is down"})
    assert item.id == "alert:RedisUnavailable"
    assert item.summary == "Redis is down"


def test_evidence_bundle_prompt_text_is_deterministic() -> None:
    bundle = build_evidence_bundle([EvidenceItem(id="a", source="s", summary="sum", data={"k": "v"})])
    text1 = bundle.as_prompt_text()
    text2 = bundle.as_prompt_text()
    assert text1 == text2
    assert "[a] (s) sum" in text1


# ---------------------------------------------------------------------------
# Runbooks
# ---------------------------------------------------------------------------


def test_find_runbook_matches_known_incident_type() -> None:
    runbook = find_runbook("db_outage")
    assert runbook is not None
    assert runbook.id == "RB-DB-001"


def test_find_runbook_returns_none_for_unknown_incident_type() -> None:
    assert find_runbook("totally_made_up_incident") is None


# ---------------------------------------------------------------------------
# Mock provider — deterministic offline mode
# ---------------------------------------------------------------------------


def test_mock_provider_produces_valid_grounded_analysis() -> None:
    provider = MockIncidentCopilotProvider()
    evidence = build_evidence_bundle(evidence_from_reliability_result(_passing_scenario()))
    analysis = provider.analyze(evidence, incident_type_hint="db_outage")

    assert isinstance(analysis, IncidentAnalysis)
    assert analysis.incident_type == "db_outage"
    assert analysis.recommended_runbook == "RB-DB-001"
    assert analysis.requires_human_approval is True
    assert analysis.evidence_ids_referenced, "must cite at least one real evidence id"
    for evidence_id in analysis.evidence_ids_referenced:
        assert evidence.get(evidence_id) is not None, f"cited evidence id {evidence_id} must exist in the bundle"


def test_mock_provider_escalates_severity_on_multiple_failed_steps() -> None:
    provider = MockIncidentCopilotProvider()
    evidence = build_evidence_bundle(evidence_from_reliability_result(_failing_scenario()))
    analysis = provider.analyze(evidence, incident_type_hint="poison_event")
    assert analysis.severity == "SEV-1"


def test_mock_provider_flags_insufficient_evidence_rather_than_guessing() -> None:
    provider = MockIncidentCopilotProvider()
    empty_bundle = build_evidence_bundle([])
    analysis = provider.analyze(empty_bundle, incident_type_hint="mystery_incident")
    assert analysis.insufficient_evidence is True
    assert analysis.probable_root_causes == []
    assert analysis.severity == "SEV-4"


def test_mock_provider_is_fully_deterministic() -> None:
    provider = MockIncidentCopilotProvider()
    evidence = build_evidence_bundle(evidence_from_reliability_result(_passing_scenario()))
    first = provider.analyze(evidence, incident_type_hint="db_outage")
    second = provider.analyze(evidence, incident_type_hint="db_outage")
    assert first.model_dump() == second.model_dump()


# ---------------------------------------------------------------------------
# Provider selection / offline-by-default
# ---------------------------------------------------------------------------


def test_get_default_provider_is_mock_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("AI_COPILOT_PROVIDER", raising=False)
    provider = get_default_provider()
    assert isinstance(provider, MockIncidentCopilotProvider)


def test_get_default_provider_rejects_unknown_provider_name(monkeypatch) -> None:
    monkeypatch.setenv("AI_COPILOT_PROVIDER", "not-a-real-provider")
    with pytest.raises(ValueError, match="unknown AI_COPILOT_PROVIDER"):
        get_default_provider()


def test_anthropic_provider_unavailable_without_api_key(monkeypatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(ProviderUnavailableError):
        AnthropicIncidentCopilotProvider()


def test_platform_works_offline_with_no_ai_api_key_configured(monkeypatch) -> None:
    """The platform must run normally with the AI feature fully offline —
    the actual end-to-end check that no API key is required at all.
    """
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("AI_COPILOT_PROVIDER", raising=False)
    evidence = build_evidence_bundle(evidence_from_reliability_result(_passing_scenario()))
    analysis = run_incident_analysis(evidence, incident_type_hint="db_outage")
    assert analysis.requires_human_approval is True


# ---------------------------------------------------------------------------
# Orchestrator: malformed/timeout/unavailable provider behavior
# ---------------------------------------------------------------------------


class _MalformedProvider(IncidentCopilotProvider):
    def analyze(self, evidence, incident_type_hint=None):  # type: ignore[override]
        return {"not": "a real IncidentAnalysis"}  # type: ignore[return-value]


def test_run_incident_analysis_rejects_a_malformed_provider_response() -> None:
    evidence = build_evidence_bundle([])
    with pytest.raises(TypeError, match="expected IncidentAnalysis"):
        run_incident_analysis(evidence, provider=_MalformedProvider())


class _TimeoutProvider(IncidentCopilotProvider):
    def analyze(self, evidence, incident_type_hint=None):  # type: ignore[override]
        from ai_incident_copilot.provider import ProviderTimeoutError

        raise ProviderTimeoutError("simulated timeout")


def test_run_incident_analysis_propagates_a_provider_timeout() -> None:
    from ai_incident_copilot.provider import ProviderTimeoutError

    evidence = build_evidence_bundle([])
    with pytest.raises(ProviderTimeoutError):
        run_incident_analysis(evidence, provider=_TimeoutProvider())


def test_run_incident_analysis_end_to_end_with_mock_provider() -> None:
    evidence = build_evidence_bundle(evidence_from_reliability_result(_passing_scenario()))
    analysis = run_incident_analysis(evidence, incident_type_hint="db_outage", provider=MockIncidentCopilotProvider())
    assert analysis.incident_type == "db_outage"
    assert analysis.recommended_runbook == "RB-DB-001"
