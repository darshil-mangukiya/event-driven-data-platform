from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

StepStatus = Literal["verified", "simulated", "not_run", "failed"]

# "verified"  — real code executed against the real platform (or, when live
#               infra wasn't available, against the platform's actual
#               transformation functions with deterministic local input — not a
#               narrative, an executed assertion).
# "simulated" — the expected behavior is described/computed but the
#               underlying infra call was not made (e.g. no Docker daemon).
# "not_run"   — deliberately skipped (a step requiring infra this run didn't
#               attempt to reach).
# "failed"    — the step ran and the assertion did not hold; this makes the
#               whole scenario "failed", not swept under "simulated".


@dataclass
class StepResult:
    name: str
    status: StepStatus
    detail: str
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass
class ScenarioResult:
    scenario_id: str
    title: str
    component: str
    expected_behavior: str
    detection_method: str
    impact: str
    root_cause: str
    recovery: str
    corrective_action: str
    preventive_control: str
    steps: list[StepResult]
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ended_at: datetime | None = None

    @property
    def observed_behavior(self) -> str:
        return " | ".join(f"[{step.status}] {step.name}: {step.detail}" for step in self.steps)

    @property
    def overall_status(self) -> str:
        if any(step.status == "failed" for step in self.steps):
            return "failed"
        if all(step.status == "not_run" for step in self.steps):
            return "not_run"
        return "passed"

    @property
    def validation_status(self) -> str:
        """Human label for the incident report, distinct from CI-style pass/fail."""
        statuses = {step.status for step in self.steps}
        if "failed" in statuses:
            return "FAILED — one or more assertions did not hold"
        if statuses == {"not_run"}:
            return "NOT RUN — no steps executed (infra unavailable)"
        if "simulated" in statuses and "verified" not in statuses:
            return "SIMULATED — expected behavior computed, not executed against live infra"
        if "simulated" in statuses:
            return "PARTIALLY VERIFIED — executed and simulated steps are present"
        return "VERIFIED — all steps executed"

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "scenario_id": self.scenario_id,
            "title": self.title,
            "component": self.component,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "overall_status": self.overall_status,
            "validation_status": self.validation_status,
            "expected_behavior": self.expected_behavior,
            "observed_behavior": self.observed_behavior,
            "detection_method": self.detection_method,
            "impact": self.impact,
            "root_cause": self.root_cause,
            "recovery": self.recovery,
            "corrective_action": self.corrective_action,
            "preventive_control": self.preventive_control,
            "steps": [
                {
                    "name": step.name,
                    "status": step.status,
                    "detail": step.detail,
                    "evidence": step.evidence,
                }
                for step in self.steps
            ],
        }
