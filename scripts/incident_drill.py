from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_incidents(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text())["incidents"]


def drill_plan(incident: dict[str, Any]) -> dict[str, Any]:
    return {
        "incident_id": incident["id"],
        "title": incident["title"],
        "severity": incident["severity"],
        "owner": incident["owner"],
        "signals": incident["signals"],
        "slo_burn_rate": round(incident["error_budget_impact_percent"] / max(incident["duration_minutes"] / 60, 0.25), 2),
        "timeline": [
            {"minute": 0, "action": "acknowledge alert and assign incident commander"},
            {"minute": 5, "action": incident["first_response"]},
            {"minute": 15, "action": incident["mitigation"]},
            {"minute": incident["duration_minutes"], "action": "confirm recovery and start postmortem notes"},
        ],
        "postmortem_required": incident["severity"] in {"sev1", "sev2"},
    }


def run_drills(incidents: list[dict[str, Any]], *, incident_id: str | None = None) -> list[dict[str, Any]]:
    selected = [incident for incident in incidents if incident_id in {None, incident["id"]}]
    return [drill_plan(incident) for incident in selected]


def main() -> None:
    parser = argparse.ArgumentParser(description="Render deterministic incident drill plans.")
    parser.add_argument("--incidents", default="incidents/scenarios.json")
    parser.add_argument("--incident-id", default=None)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    result = {"drills": run_drills(load_incidents(Path(args.incidents)), incident_id=args.incident_id)}
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
