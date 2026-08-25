from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def import_service_main(service_name: str):
    service_path = PROJECT_ROOT / "services" / service_name
    for module_name in list(sys.modules):
        if module_name == "app" or module_name.startswith("app."):
            del sys.modules[module_name]
    sys.path.insert(0, str(service_path))
    try:
        return importlib.import_module("app.main")
    finally:
        try:
            sys.path.remove(str(service_path))
        except ValueError:
            pass


def test_openapi_contract_includes_core_platform_endpoints() -> None:
    expected_paths = {
        "ingestion-service": {"/events", "/events/batch", "/generate/demo", "/health", "/metrics"},
        "analytics-service": {
            "/metrics/revenue",
            "/metrics/customers",
            "/metrics/churn",
            "/metrics/retention",
            "/metrics/marketing_roi",
            "/metrics/product_performance",
            "/metrics/payment_success",
            "/metrics/event_throughput",
            "/metrics/tenant_health_score",
            "/alerts",
            "/system/status",
        },
        "metadata-service": {"/auth/token", "/tenants", "/tenants/{tenant_id}/users", "/tenants/{tenant_id}/products"},
        "processing-service": {"/health", "/system/status", "/metrics"},
        "demo-dashboard": {"/", "/api/dashboard", "/health", "/metrics"},
        "ops-console": {"/", "/api/ops", "/health", "/metrics"},
    }

    for service_name, paths in expected_paths.items():
        module = import_service_main(service_name)
        openapi = module.app.openapi()
        missing = paths - set(openapi["paths"])
        assert missing == set(), f"{service_name} missing OpenAPI paths {missing}"


def test_exported_openapi_artifacts_include_idempotency_contract() -> None:
    openapi = (PROJECT_ROOT / "api" / "openapi" / "ingestion-service.openapi.json").read_text()
    fixture = (PROJECT_ROOT / "api" / "fixtures" / "ingestion_order_request.json").read_text()

    assert "idempotency_key" in openapi
    assert "idempotency_key" in fixture


class FakeCache:
    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def incr_rate_limit(self, key: str, ttl_seconds: int = 60) -> int:
        return 1

    async def get_json(self, key: str) -> Any | None:
        return None

    async def set_json(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        return None

    def stats(self) -> dict[str, Any]:
        return {"hits": 0, "misses": 1, "hit_rate": 0}


class FakePostgres:
    async def execute(self, *args: Any, **kwargs: Any) -> str:
        return "INSERT 0 1"


class FakeAnalyticsRepository:
    async def revenue(self, **kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "metric_date": "2026-05-25",
                "gross_revenue": 120.0,
                "net_revenue": 100.0,
                "order_count": 4,
                "units_sold": 7,
                "average_order_value": 25.0,
            }
        ]

    async def tenant_health_score(self, *, tenant_id: str) -> dict[str, Any]:
        return {
            "tenant_id": tenant_id,
            "tenant_health_score": 96.5,
            "payment_successes": 10,
            "payment_failures": 1,
            "events_processed": 500,
        }

    async def payment_success(self, **kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "metric_date": "2026-05-25",
                "payment_success_count": 96,
                "payment_failure_count": 4,
                "payment_attempt_count": 100,
                "payment_success_rate": 0.96,
            }
        ]

    async def event_throughput(self, **kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "metric_date": "2026-05-25",
                "events_processed": 500,
                "raw_event_count": 505,
                "order_count": 120,
                "payment_success_count": 96,
                "payment_failure_count": 4,
            }
        ]


def test_analytics_revenue_response_shape_and_cache_header_scaffold(monkeypatch) -> None:
    module = import_service_main("analytics-service")
    monkeypatch.setattr(module, "cache", FakeCache())
    monkeypatch.setattr(module, "postgres", FakePostgres())
    monkeypatch.setattr(module, "repository", FakeAnalyticsRepository())

    client = TestClient(module.app)
    response = client.get(
        "/metrics/revenue",
        params={"tenant_id": "tenant_demo", "limit": 1},
        headers={"X-Tenant-ID": "tenant_demo", "X-User-ID": "contract-test"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["tenant_id"] == "tenant_demo"
    assert payload["metric"] == "revenue"
    assert payload["cached"] is False
    assert payload["count"] == 1
    assert {"metric_date", "net_revenue", "order_count"} <= set(payload["data"][0])


def test_analytics_api_blocks_cross_tenant_access(monkeypatch) -> None:
    module = import_service_main("analytics-service")
    monkeypatch.setattr(module, "cache", FakeCache())
    monkeypatch.setattr(module, "postgres", FakePostgres())
    monkeypatch.setattr(module, "repository", FakeAnalyticsRepository())

    client = TestClient(module.app)
    response = client.get(
        "/metrics/revenue",
        params={"tenant_id": "tenant_enterprise"},
        headers={"X-Tenant-ID": "tenant_demo", "X-User-ID": "contract-test"},
    )

    assert response.status_code == 403
    assert "cannot access tenant" in response.json()["detail"]


def test_analytics_payment_success_and_event_throughput_endpoints(monkeypatch) -> None:
    module = import_service_main("analytics-service")
    monkeypatch.setattr(module, "cache", FakeCache())
    monkeypatch.setattr(module, "postgres", FakePostgres())
    monkeypatch.setattr(module, "repository", FakeAnalyticsRepository())

    client = TestClient(module.app)
    headers = {"X-Tenant-ID": "tenant_demo", "X-User-ID": "contract-test"}

    payment = client.get("/metrics/payment_success", params={"tenant_id": "tenant_demo"}, headers=headers)
    throughput = client.get("/metrics/event_throughput", params={"tenant_id": "tenant_demo"}, headers=headers)

    assert payment.status_code == 200
    assert payment.json()["metric"] == "payment_success"
    assert {"payment_success_count", "payment_failure_count", "payment_success_rate"} <= set(
        payment.json()["data"][0]
    )

    assert throughput.status_code == 200
    assert throughput.json()["metric"] == "event_throughput"
    assert {"events_processed", "raw_event_count", "metric_date"} <= set(throughput.json()["data"][0])
