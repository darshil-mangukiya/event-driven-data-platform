"""Strict, typed output schema for the AI Incident Copilot. Both the mock
provider and any external LLM provider must produce a response that validates
against this schema — a malformed/hallucinated shape is rejected before
it ever reaches a human, not passed through.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

Severity = Literal["SEV-1", "SEV-2", "SEV-3", "SEV-4"]


class RootCause(BaseModel):
    cause: str
    confidence: float = Field(ge=0.0, le=1.0)
    supporting_evidence: list[str] = Field(
        default_factory=list,
        description="Evidence item ids (see EvidenceBundle) this cause is grounded in. Required for confidence > 0.3.",
    )

    @field_validator("confidence")
    @classmethod
    def _confidence_range(cls, value: float) -> float:
        if not (0.0 <= value <= 1.0):
            raise ValueError("confidence must be between 0.0 and 1.0")
        return value


class IncidentAnalysis(BaseModel):
    incident_type: str
    severity: Severity
    affected_component: str
    probable_root_causes: list[RootCause] = Field(default_factory=list)
    affected_tenants: list[str] = Field(default_factory=list)
    downstream_impact: list[str] = Field(default_factory=list)
    recommended_runbook: str | None = None
    recommended_actions: list[str] = Field(default_factory=list)
    requires_human_approval: bool = True
    evidence_ids_referenced: list[str] = Field(
        default_factory=list,
        description="Evidence item ids this analysis actually cites — grounding check, see validate_is_grounded().",
    )
    insufficient_evidence: bool = Field(
        default=False,
        description="True when the copilot could not find enough real evidence to support a confident conclusion — "
        "must be set instead of inventing a root cause. See ai_incident_copilot.grounding.",
    )

    @field_validator("requires_human_approval")
    @classmethod
    def _human_approval_always_true_for_now(cls, value: bool) -> bool:
        # A hard platform invariant rather than a configurable default: this copilot never
        # autonomously executes anything (see AI_CONTROL_BOUNDARIES.md), so
        # every analysis it produces requires human review before any
        # recommended_actions are carried out. A provider claiming
        # otherwise is a bug in the provider, not a valid analysis.
        if value is not True:
            raise ValueError("requires_human_approval must always be True — this copilot never acts autonomously")
        return value
