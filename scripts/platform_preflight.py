from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class Check:
    name: str
    command: list[str]


def preflight_checks(python: str = sys.executable) -> list[Check]:
    return [
        Check("event_contracts", [python, "scripts/validate_event_contracts.py"]),
        Check("contract_compatibility", [python, "scripts/check_contract_compatibility.py"]),
        Check("catalog", [python, "scripts/validate_catalog.py"]),
        Check("samples", [python, "scripts/validate_sample_artifacts.py"]),
        Check("privacy", [python, "scripts/validate_privacy_catalog.py"]),
        Check("schema_drift", [python, "scripts/schema_drift_report.py"]),
        Check("rls", [python, "scripts/validate_tenant_rls.py"]),
        Check("metric_contracts", [python, "scripts/validate_metric_contracts.py"]),
        Check("resilience_probe", [python, "scripts/resilience_probe.py", "--dry-run"]),
    ]


def run_preflight(*, dry_run: bool = False) -> dict[str, object]:
    results = []
    for check in preflight_checks():
        if dry_run:
            results.append({"name": check.name, "status": "dry_run", "command": check.command})
            continue
        completed = subprocess.run(check.command, capture_output=True, text=True, check=False)
        results.append(
            {
                "name": check.name,
                "status": "passed" if completed.returncode == 0 else "failed",
                "returncode": completed.returncode,
                "stdout": completed.stdout.strip(),
                "stderr": completed.stderr.strip(),
            }
        )
    failed = [result for result in results if result["status"] == "failed"]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "failed" if failed else "passed",
        "checks": results,
    }


def render_markdown(report: dict[str, object]) -> str:
    lines = ["# Release Readiness Report", "", f"Status: `{report['status']}`", ""]
    lines.extend(["| Check | Status |", "| --- | --- |"])
    for check in report["checks"]:  # type: ignore[index]
        lines.append(f"| {check['name']} | {check['status']} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local platform release preflight checks.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--output-md", default=None)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    report = run_preflight(dry_run=args.dry_run)
    if args.output_json:
        Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_json).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    if args.output_md:
        Path(args.output_md).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_md).write_text(render_markdown(report))
    print(json.dumps(report, indent=2 if args.pretty else None, sort_keys=True))
    if report["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
