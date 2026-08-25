from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CAPABILITY_MATRIX = [
    {
        "capability": "Microservices",
        "evidence": ["services/ingestion-service", "services/processing-service", "services/analytics-service", "services/metadata-service", "services/ops-console"],
        "tests": ["tests/test_api_contracts.py"],
    },
    {
        "capability": "Event contracts and compatibility",
        "evidence": ["contracts/registry.json", "contracts/schemas/v1", "contracts/schemas/v2", "scripts/check_contract_compatibility.py"],
        "tests": ["tests/test_event_contracts.py", "tests/test_reliability_governance_tooling.py"],
    },
    {
        "capability": "Replay safety",
        "evidence": ["docs/idempotency-replay-safety.md", "services/processing-service/app/worker.py", "services/processing-service/app/repository.py"],
        "tests": ["tests/integration/test_event_processor_flow.py"],
    },
    {
        "capability": "Outbox/inbox operations",
        "evidence": ["sql/outbox", "scripts/outbox_dispatch_plan.py", "docs/outbox-inbox-pattern.md"],
        "tests": ["tests/test_platform_hardening_tooling.py"],
    },
    {
        "capability": "Governance and privacy",
        "evidence": ["governance/pii_classification.json", "sql/privacy/tenant_erasure_plan.sql", "docs/privacy-governance.md"],
        "tests": ["tests/test_platform_hardening_tooling.py"],
    },
    {
        "capability": "Release readiness",
        "evidence": ["scripts/platform_preflight.py", "docs/release-readiness.md"],
        "tests": ["tests/test_platform_packaging.py"],
    },
    {
        "capability": "Tenant onboarding and platform CLI",
        "evidence": ["platform_cli", "docs/tenant-onboarding.md", "examples/internal_consumers/team_api_consumers.py"],
        "tests": ["tests/test_platform_cli_and_tenant_onboarding.py"],
    },
    {
        "capability": "Traceability and checkpointing",
        "evidence": ["database/migrations/versions/0005_traceability_watermarks_audit.py", "database/init/001_schema.sql"],
        "tests": ["tests/test_platform_cli_and_tenant_onboarding.py", "tests/test_event_contracts.py"],
    },
]


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def generate_bundle(output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    bundle = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "capability_count": len(CAPABILITY_MATRIX),
        "capabilities": CAPABILITY_MATRIX,
    }
    _write_json(output_dir / "capability_matrix.json", bundle)
    _write_json(output_dir / "service_health_summary.json", service_health_summary())
    _write_json(output_dir / "sample_api_responses.json", sample_api_responses())
    _write_json(output_dir / "benchmark_summary.json", benchmark_summary())
    _write_json(output_dir / "data_quality_summary.json", data_quality_summary())
    _write_json(output_dir / "reconciliation_summary.json", reconciliation_summary())
    _write_json(output_dir / "docker_services.json", docker_services_inventory())
    _write_json(output_dir / "test_result_summary.json", test_result_summary())
    (output_dir / "README.md").write_text(render_readme(bundle))
    return bundle


def service_health_summary() -> dict[str, Any]:
    return {
        "mode": "static_evidence",
        "services": [
            {"service": "ingestion-service", "health_endpoint": "http://localhost:8001/health", "status_source": "FastAPI /health"},
            {"service": "processing-service", "health_endpoint": "http://localhost:8002/health", "status_source": "FastAPI /health"},
            {"service": "analytics-service", "health_endpoint": "http://localhost:8003/health", "status_source": "FastAPI /health"},
            {"service": "metadata-service", "health_endpoint": "http://localhost:8004/health", "status_source": "FastAPI /health"},
            {"service": "ops-console", "health_endpoint": "http://localhost:8006/health", "status_source": "FastAPI /health"},
        ],
        "runtime_note": "Run docker compose and scripts/api_smoke_test.py for live health evidence.",
    }


def sample_api_responses() -> dict[str, Any]:
    root = Path(".")
    return {
        "analytics_revenue_response": _read_json(root / "api/fixtures/analytics_revenue_response.json"),
        "ingestion_order_request": _read_json(root / "api/fixtures/ingestion_order_request.json"),
        "ingestion_order_response": _read_json(root / "api/fixtures/ingestion_order_response.json"),
        "metadata_token_request": _read_json(root / "api/fixtures/metadata_token_request.json"),
    }


def benchmark_summary() -> dict[str, Any]:
    sample = _read_json(Path("samples/benchmarks/local_ingestion_sample.json"))
    return {
        "mode": "sample_local_benchmark",
        "sample": sample,
        "honesty_note": "This is local benchmark evidence. Production throughput requires separate distributed load validation.",
        "next_commands": [
            "python scripts/load_test_events.py --batches 20 --batch-size 50",
            "k6 run benchmarks/k6/analytics_read_load.js",
        ],
    }


def data_quality_summary() -> dict[str, Any]:
    sample = _read_json(Path("samples/quality/tenant_demo_quality_sample.json"))
    return {
        "mode": "sample_quality_output",
        "sample": sample,
        "next_command": "python scripts/run_data_quality_checks.py --tenant-id tenant_demo --pretty",
    }


def reconciliation_summary() -> dict[str, Any]:
    return {
        "mode": "dry_run",
        "checks": [
            "raw event count versus processed records",
            "tenant daily aggregate revenue deltas",
            "order count and units sold deltas",
            "DLQ and rejected event review",
        ],
        "next_command": "PYTHONPATH=services/shared python scripts/reconciliation_summary.py --tenant-id tenant_demo --days 7 --dry-run --pretty",
    }


def docker_services_inventory() -> dict[str, Any]:
    return {
        "mode": "compose_inventory",
        "compose_file": "docker-compose.yml",
        "services": [
            "postgres",
            "redis",
            "kafka",
            "spark-master",
            "minio",
            "ingestion-service",
            "processing-service",
            "analytics-service",
            "metadata-service",
            "demo-dashboard",
            "ops-console",
            "prometheus",
            "grafana",
        ],
        "live_check": "docker compose -f docker-compose.yml ps",
    }


def test_result_summary() -> dict[str, Any]:
    return {
        "mode": "command_evidence",
        "commands": [
            "pytest -q",
            "ruff check .",
            "python -m compileall services scripts tests platform_cli",
            "PYTHONPATH=services/shared python scripts/validate_event_contracts.py",
            "python -m platform_cli health check --dry-run --pretty",
        ],
    }


def render_readme(bundle: dict[str, object]) -> str:
    lines = [
        "# Platform Evidence Bundle",
        "",
        "This generated bundle indexes implemented capabilities and links them to code, docs, tests, sample API responses, benchmark evidence, data quality output, and operational readiness checks.",
        "",
        "Generated files:",
        "",
        "- `capability_matrix.json`",
        "- `service_health_summary.json`",
        "- `sample_api_responses.json`",
        "- `benchmark_summary.json`",
        "- `data_quality_summary.json`",
        "- `reconciliation_summary.json`",
        "- `docker_services.json`",
        "- `test_result_summary.json`",
        "",
        "| Capability | Evidence | Tests |",
        "| --- | --- | --- |",
    ]
    for item in bundle["capabilities"]:  # type: ignore[index]
        evidence = "<br>".join(f"`{path}`" for path in item["evidence"])
        tests = "<br>".join(f"`{path}`" for path in item["tests"])
        lines.append(f"| {item['capability']} | {evidence} | {tests} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a local platform evidence bundle.")
    parser.add_argument("--output-dir", default="evidence/validation")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    bundle = generate_bundle(Path(args.output_dir))
    print(json.dumps(bundle, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
