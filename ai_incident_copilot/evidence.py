"""Builds a structured incident evidence bundle from actual platform
signals — real `reliability.models.ScenarioResult` objects (from
`reliability/scenarios/*.py`, this project's own deterministic failure
exercises), not invented metrics. The copilot is only ever handed this
bundle, never raw free-text — every fact it can cite has a stable id it
must reference (see schema.IncidentAnalysis.evidence_ids_referenced and
grounding.validate_is_grounded).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class EvidenceItem:
    id: str
    source: str
    summary: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidenceBundle:
    items: list[EvidenceItem]

    def as_prompt_text(self) -> str:
        """A deterministic, ordered rendering — real providers see exactly
        this text (plus the schema); the mock provider also parses this
        same structure, so both paths are grounded in identical input.
        """
        lines = []
        for item in self.items:
            lines.append(f"[{item.id}] ({item.source}) {item.summary}")
            for key, value in item.data.items():
                lines.append(f"    {key}: {value}")
        return "\n".join(lines)

    def get(self, item_id: str) -> EvidenceItem | None:
        return next((i for i in self.items if i.id == item_id), None)


def evidence_from_reliability_result(scenario_result: Any) -> list[EvidenceItem]:
    """Real reliability.models.ScenarioResult -> evidence items — one per
    step, exactly reflecting what that scenario's real code actually
    verified (or didn't), never summarized/lossy.
    """
    items: list[EvidenceItem] = []
    items.append(
        EvidenceItem(
            id=f"reliability:{scenario_result.scenario_id}:summary",
            source="reliability_exercise",
            summary=f"{scenario_result.title}: {scenario_result.expected_behavior}",
            data={
                "scenario_id": scenario_result.scenario_id,
                "component": scenario_result.component,
                "overall_status": scenario_result.overall_status,
                "root_cause": scenario_result.root_cause,
            },
        )
    )
    for step in scenario_result.steps:
        items.append(
            EvidenceItem(
                id=f"reliability:{scenario_result.scenario_id}:step:{step.name}",
                source="reliability_exercise_step",
                summary=step.detail,
                data={"status": step.status, "evidence": step.evidence},
            )
        )
    return items


def evidence_from_reconciliation_results(results: list[dict[str, Any]]) -> list[EvidenceItem]:
    """Real scripts/reconcile_metrics.py output rows -> evidence items."""
    items: list[EvidenceItem] = []
    for i, row in enumerate(results):
        items.append(
            EvidenceItem(
                id=f"reconciliation:{row.get('check_name', 'unknown')}:{row.get('tenant_id', 'unknown')}:{i}",
                source="reconciliation_audit",
                summary=f"{row.get('check_name')} for {row.get('tenant_id')} on {row.get('metric_date')}: {row.get('status')}",
                data=row,
            )
        )
    return items


def evidence_from_prometheus_alert(alert_name: str, labels: dict[str, str], annotations: dict[str, str]) -> EvidenceItem:
    """A real firing Prometheus alert (monitoring/alert_rules.yml) -> one
    evidence item. This is the actual DETECTION signal in the real flow —
    the copilot runs *after* this, never before it.
    """
    return EvidenceItem(
        id=f"alert:{alert_name}",
        source="prometheus_alert",
        summary=annotations.get("summary", alert_name),
        data={"labels": labels, **annotations},
    )


def build_evidence_bundle(items: list[EvidenceItem]) -> EvidenceBundle:
    return EvidenceBundle(items=items)
