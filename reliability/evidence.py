"""Write structured incident-simulation evidence for a completed reliability exercise.

Output layout, under ``artifacts/reliability/<run_id>/``:

    scenario.json        full ScenarioResult dump (machine-readable)
    incident_report.md   the required incident-report fields, human-readable
    metrics.json         evidence dicts pulled out of each step
    validation.json       per-step status list — what actually ran vs. was simulated/skipped
    remediation.md        symptom / detection / root cause / recovery / corrective / preventive

These are local resilience-test artifacts, not real production incident
reports — every generated file says so explicitly.
"""

from __future__ import annotations

import json
from pathlib import Path

from reliability.models import ScenarioResult

DEFAULT_ARTIFACTS_ROOT = Path("artifacts/reliability")


def write_evidence(result: ScenarioResult, *, artifacts_root: Path | None = None) -> Path:
    root = (artifacts_root or DEFAULT_ARTIFACTS_ROOT) / result.run_id
    root.mkdir(parents=True, exist_ok=True)

    (root / "scenario.json").write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True, default=str))

    (root / "metrics.json").write_text(
        json.dumps(
            {step.name: step.evidence for step in result.steps if step.evidence},
            indent=2,
            sort_keys=True,
            default=str,
        )
    )

    (root / "validation.json").write_text(
        json.dumps(
            {
                "overall_status": result.overall_status,
                "validation_status": result.validation_status,
                "steps": [{"name": s.name, "status": s.status, "detail": s.detail} for s in result.steps],
            },
            indent=2,
            sort_keys=True,
        )
    )

    (root / "incident_report.md").write_text(_incident_report_md(result, root))
    (root / "remediation.md").write_text(_remediation_md(result))

    return root


def _incident_report_md(result: ScenarioResult, root: Path) -> str:
    steps_table = "\n".join(
        f"| {step.name} | {step.status} | {step.detail} |" for step in result.steps
    )
    evidence_files = ", ".join(
        p.name for p in sorted(root.glob("*")) if p.name != "incident_report.md"
    )
    return f"""# Reliability Exercise Report — {result.title}

_This is a **local reliability exercise / failure simulation**, not a real
production incident. It was run deliberately against this local platform to
prove a specific failure-handling behavior._

- **Incident ID**: {result.run_id}
- **Scenario**: {result.scenario_id} — {result.title}
- **Timestamp**: {result.started_at.isoformat()} → {result.ended_at.isoformat() if result.ended_at else "(in progress)"}
- **Affected component**: {result.component}
- **Expected behavior**: {result.expected_behavior}
- **Observed behavior**: {result.observed_behavior}
- **Detection method**: {result.detection_method}
- **Impact**: {result.impact}
- **Root cause**: {result.root_cause}
- **Recovery**: {result.recovery}
- **Corrective action**: {result.corrective_action}
- **Prevention**: {result.preventive_control}
- **Validation status**: {result.validation_status}
- **Evidence files**: {evidence_files}

## Steps executed

| Step | Status | Detail |
| --- | --- | --- |
{steps_table}
"""


def _remediation_md(result: ScenarioResult) -> str:
    return f"""# Root-Cause Summary — {result.title}

_Local reliability exercise ({result.scenario_id}), run {result.run_id}._

- **Symptom**: {result.impact}
- **Detection**: {result.detection_method}
- **Probable Root Cause**: {result.root_cause}
- **Evidence**: see `scenario.json`, `metrics.json`, `validation.json` in this directory.
- **Recovery Action**: {result.recovery}
- **Corrective Action**: {result.corrective_action}
- **Preventive Control**: {result.preventive_control}
"""
