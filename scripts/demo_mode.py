from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class DemoStep:
    name: str
    command: list[str]
    required: bool = True
    description: str = ""


def build_demo_plan(args: argparse.Namespace) -> list[DemoStep]:
    steps: list[DemoStep] = []
    if not args.skip_stack:
        steps.append(
            DemoStep(
                name="start_stack",
                command=["docker", "compose", "-f", "docker-compose.yml", "up", "--build", "-d"],
                description="Start Docker Compose services.",
            )
        )
    steps.extend(
        [
            DemoStep(
                name="wait_for_services",
                command=["python", "scripts/wait_for_services.py", "--timeout-seconds", str(args.timeout_seconds)],
                required=False,
                description="Wait for API services to respond.",
            ),
            DemoStep(
                name="generate_synthetic_events",
                command=[
                    "python",
                    "scripts/generate_synthetic_events_v2.py",
                    "--count",
                    str(args.synthetic_count),
                    "--output",
                    "data/synthetic/demo_events.jsonl",
                ],
                description="Generate tenant-patterned local JSONL events.",
            ),
            DemoStep(
                name="local_e2e",
                command=["python", "scripts/run_local_e2e.py", "--settle-seconds", str(args.settle_seconds)],
                required=False,
                description="Issue token, ingest one event, and query analytics API.",
            ),
            DemoStep(
                name="quality_checks",
                command=["python", "scripts/run_data_quality_checks.py", "--pretty"],
                required=False,
                description="Run tenant data quality checks.",
            ),
            DemoStep(
                name="benchmark_report",
                command=["python", "scripts/benchmark_report.py", "--output", "docs/benchmark-evidence.md"],
                description="Render benchmark evidence report.",
            ),
        ]
    )
    return steps


def run_step(step: DemoStep, dry_run: bool) -> dict[str, object]:
    if dry_run:
        return {"name": step.name, "status": "planned", "command": step.command}
    started = time.perf_counter()
    try:
        completed = subprocess.run(step.command, check=False, text=True, capture_output=True)
    except FileNotFoundError as exc:
        status = "failed" if step.required else "skipped"
        return {
            "name": step.name,
            "status": status,
            "command": step.command,
            "required": step.required,
            "error": str(exc),
        }

    status = "passed" if completed.returncode == 0 else ("failed" if step.required else "skipped")
    return {
        "name": step.name,
        "status": status,
        "command": step.command,
        "required": step.required,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-1000:],
        "stderr_tail": completed.stderr[-1000:],
    }


def write_demo_summary(results: list[dict[str, object]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "results": results,
        "urls": {
            "ingestion_docs": "http://localhost:8001/docs",
            "processing_docs": "http://localhost:8002/docs",
            "analytics_docs": "http://localhost:8003/docs",
            "metadata_docs": "http://localhost:8004/docs",
            "demo_dashboard": "http://localhost:8005/?tenant_id=tenant_demo",
            "prometheus": "http://localhost:9090",
            "minio_console": "http://localhost:9001",
        },
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local demo workflow.")
    parser.add_argument("--skip-stack", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--synthetic-count", type=int, default=1000)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--settle-seconds", type=int, default=5)
    parser.add_argument("--summary-output", default="evidence/validation/demo-summary.json")
    args = parser.parse_args()

    if not args.skip_stack and shutil.which("docker") is None:
        args.skip_stack = True
        print("Docker not found; continuing with stack startup skipped.")

    plan = build_demo_plan(args)
    if args.dry_run:
        print(json.dumps([asdict(step) for step in plan], indent=2))
        return

    results = [run_step(step, dry_run=False) for step in plan]
    write_demo_summary(results, Path(args.summary_output))
    print(json.dumps({"summary_output": args.summary_output, "results": results}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
