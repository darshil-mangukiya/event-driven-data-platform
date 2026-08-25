from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import yaml

from scripts.docker_smoke_check import CheckResult, build_checks, format_result

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def args(**overrides: object) -> Namespace:
    values = {
        "ingestion_url": "http://localhost:8001",
        "analytics_url": "http://localhost:8003",
        "metadata_url": "http://localhost:8004",
        "airflow_url": "http://localhost:8088",
        "prometheus_url": "http://localhost:9090",
        "include_airflow": False,
        "include_prometheus": False,
    }
    values.update(overrides)
    return Namespace(**values)


def test_build_checks_defaults_to_core_service_health_and_openapi() -> None:
    checks = build_checks(args())

    urls = [check.url for check in checks]
    assert "http://localhost:8001/health" in urls
    assert "http://localhost:8001/docs" in urls
    assert "http://localhost:8003/health" in urls
    assert "http://localhost:8004/docs" in urls
    assert all("8088" not in url for url in urls)


def test_build_checks_can_include_optional_airflow_and_prometheus() -> None:
    checks = build_checks(args(include_airflow=True, include_prometheus=True))

    urls = [check.url for check in checks]
    assert "http://localhost:8088/health" in urls
    assert "http://localhost:9090/-/healthy" in urls


def test_format_result_marks_pass_and_fail() -> None:
    passed = format_result(CheckResult("analytics health", "http://localhost:8003/health", True, 200))
    failed = format_result(CheckResult("analytics health", "http://localhost:8003/health", False, error="down"))

    assert passed.startswith("[PASS]")
    assert "status=200" in passed
    assert failed.startswith("[FAIL]")
    assert "error=down" in failed


def test_all_compose_published_ports_bind_to_loopback() -> None:
    published_services: set[str] = set()

    for filename in ("docker-compose.yml", "docker-compose.airflow.yml"):
        compose = yaml.safe_load((PROJECT_ROOT / filename).read_text())
        for service_name, service in compose["services"].items():
            ports = service.get("ports", [])
            if ports:
                published_services.add(service_name)
            for port in ports:
                assert isinstance(port, str), f"{filename}:{service_name} must use an explicit host binding"
                assert port.startswith("127.0.0.1:"), f"{filename}:{service_name} publishes {port!r} beyond loopback"

    assert {"ops-console", "schema-registry", "demo-dashboard"} <= published_services
