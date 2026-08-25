"""Provider abstraction for incident analysis."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod

from ai_incident_copilot.evidence import EvidenceBundle
from ai_incident_copilot.runbooks import find_runbook
from ai_incident_copilot.schema import IncidentAnalysis, RootCause


class IncidentCopilotProvider(ABC):
    @abstractmethod
    def analyze(self, evidence: EvidenceBundle, incident_type_hint: str | None = None) -> IncidentAnalysis: ...


class ProviderTimeoutError(Exception):
    pass


class ProviderUnavailableError(Exception):
    pass


class MockIncidentCopilotProvider(IncidentCopilotProvider):
    """Deterministic rule-based classification from an evidence bundle."""

    def analyze(self, evidence: EvidenceBundle, incident_type_hint: str | None = None) -> IncidentAnalysis:
        summary_item = next((i for i in evidence.items if i.source == "reliability_exercise"), None)
        failed_steps = [i for i in evidence.items if i.source == "reliability_exercise_step" and i.data.get("status") == "failed"]
        alert_items = [i for i in evidence.items if i.source == "prometheus_alert"]

        if summary_item is None and not alert_items:
            return IncidentAnalysis(
                incident_type=incident_type_hint or "unknown",
                severity="SEV-4",
                affected_component="unknown",
                insufficient_evidence=True,
                requires_human_approval=True,
            )

        incident_type = incident_type_hint or (summary_item.data.get("scenario_id") if summary_item else "unknown")
        component = (summary_item.data.get("component") if summary_item else None) or (
            alert_items[0].data.get("labels", {}).get("job") if alert_items else "unknown"
        )

        severity: str = "SEV-3"
        if failed_steps or any("critical" in (i.data.get("labels", {}).get("severity", "")) for i in alert_items):
            severity = "SEV-2"
        if summary_item and summary_item.data.get("overall_status") == "failed" and len(failed_steps) >= 2:
            severity = "SEV-1"

        root_causes: list[RootCause] = []
        evidence_ids: list[str] = []
        if summary_item and summary_item.data.get("root_cause"):
            root_causes.append(
                RootCause(
                    cause=str(summary_item.data["root_cause"]),
                    confidence=0.75 if summary_item.data.get("overall_status") in ("passed", "failed") else 0.3,
                    supporting_evidence=[summary_item.id],
                )
            )
            evidence_ids.append(summary_item.id)
        for step in failed_steps:
            root_causes.append(RootCause(cause=f"step failed: {step.summary}", confidence=0.6, supporting_evidence=[step.id]))
            evidence_ids.append(step.id)

        runbook = find_runbook(incident_type) if incident_type else None

        return IncidentAnalysis(
            incident_type=incident_type or "unknown",
            severity=severity,  # type: ignore[arg-type]
            affected_component=component or "unknown",
            probable_root_causes=root_causes,
            downstream_impact=[i.summary for i in failed_steps],
            recommended_runbook=runbook.id if runbook else None,
            recommended_actions=(
                ["Review the linked runbook and confirm with the on-call engineer before taking action."]
                if root_causes
                else ["Insufficient evidence to recommend an action — escalate for manual investigation."]
            ),
            requires_human_approval=True,
            evidence_ids_referenced=evidence_ids,
            insufficient_evidence=not root_causes,
        )


class AnthropicIncidentCopilotProvider(IncidentCopilotProvider):
    """Unimplemented external-provider extension point."""

    def __init__(self, model: str = "claude-sonnet-5", timeout_seconds: float = 30.0) -> None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ProviderUnavailableError(
                "ANTHROPIC_API_KEY not set — AnthropicIncidentCopilotProvider requires it; "
                "the platform runs fully offline via MockIncidentCopilotProvider without it."
            )
        self._api_key = api_key
        self.model = model
        self.timeout_seconds = timeout_seconds

    def analyze(self, evidence: EvidenceBundle, incident_type_hint: str | None = None) -> IncidentAnalysis:
        raise NotImplementedError(
            "AnthropicIncidentCopilotProvider.analyze is not implemented; "
            "use MockIncidentCopilotProvider"
        )


def get_default_provider() -> IncidentCopilotProvider:
    provider_name = os.getenv("AI_COPILOT_PROVIDER", "mock").strip().lower()
    if provider_name == "mock":
        return MockIncidentCopilotProvider()
    if provider_name == "anthropic":
        return AnthropicIncidentCopilotProvider()
    raise ValueError(f"unknown AI_COPILOT_PROVIDER: {provider_name!r} (expected 'mock' or 'anthropic')")
