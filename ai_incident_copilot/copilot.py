"""Orchestrates the deterministic-detection -> evidence -> runbook ->
provider -> human-review flow. This module never detects an incident
itself — it is always called *with* evidence that already exists because
something deterministic (a reliability exercise, a Prometheus alert, a
reconciliation check) already found a real problem.
"""

from __future__ import annotations

from ai_incident_copilot.evidence import EvidenceBundle
from ai_incident_copilot.provider import (
    IncidentCopilotProvider,
    ProviderTimeoutError,
    ProviderUnavailableError,
    get_default_provider,
)
from ai_incident_copilot.schema import IncidentAnalysis


def run_incident_analysis(
    evidence: EvidenceBundle,
    *,
    incident_type_hint: str | None = None,
    provider: IncidentCopilotProvider | None = None,
) -> IncidentAnalysis:
    """The single entry point. Never raises for "the provider produced a
    bad analysis" — schema validation happens inside the provider (mock)
    or via pydantic on the way out (real provider); a provider-level
    failure (timeout, unavailable) does propagate, since silently
    returning a fabricated analysis on failure would be worse than
    failing loudly.
    """
    active_provider = provider or get_default_provider()
    try:
        analysis = active_provider.analyze(evidence, incident_type_hint=incident_type_hint)
    except (ProviderTimeoutError, ProviderUnavailableError):
        raise
    if not isinstance(analysis, IncidentAnalysis):
        raise TypeError(f"provider returned {type(analysis)}, expected IncidentAnalysis")
    return analysis
