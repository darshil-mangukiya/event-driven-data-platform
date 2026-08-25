"""Run the AI Incident Copilot against a reliability exercise result:
deterministic detection -> evidence -> copilot output.
usable from the CLI. Defaults to the mock provider (no API key needed).

Usage:
    python scripts/ai_incident_copilot_run.py --scenario db-outage --pretty
    python scripts/ai_incident_copilot_run.py --scenario poison-event --pretty
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "services" / "shared"))

from ai_incident_copilot.copilot import run_incident_analysis  # noqa: E402
from ai_incident_copilot.evidence import (  # noqa: E402
    build_evidence_bundle,
    evidence_from_reliability_result,
)

SCENARIO_MODULES = {
    "db-outage": "reliability.scenarios.db_outage",
    "redis-outage": "reliability.scenarios.redis_outage",
    "poison-event": "reliability.scenarios.poison_event",
    "duplicate-event": "reliability.scenarios.duplicate_event",
    "late-event": "reliability.scenarios.late_event",
    "consumer-lag": "reliability.scenarios.consumer_lag",
    "reconciliation-mismatch": "reliability.scenarios.reconciliation_mismatch",
    "consumer-interruption": "reliability.scenarios.consumer_interruption",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the AI Incident Copilot against a reliability exercise result.")
    parser.add_argument("--scenario", required=True, choices=sorted(SCENARIO_MODULES))
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    import importlib

    module = importlib.import_module(SCENARIO_MODULES[args.scenario])
    scenario_result = module.run()

    evidence = build_evidence_bundle(evidence_from_reliability_result(scenario_result))
    analysis = run_incident_analysis(evidence, incident_type_hint=args.scenario.replace("-", "_"))

    print(
        json.dumps(
            {
                "evidence_bundle": evidence.as_prompt_text(),
                "analysis": analysis.model_dump(),
            },
            indent=2 if args.pretty else None,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
