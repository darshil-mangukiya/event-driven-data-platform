from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any

from platform_shared.schemas import idempotent_event_id

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SERVICES = [
    "ingestion-service",
    "processing-service",
    "analytics-service",
    "metadata-service",
    "demo-dashboard",
    "ops-console",
]


def import_service_app(service_name: str) -> Any:
    service_path = PROJECT_ROOT / "services" / service_name
    for module_name in list(sys.modules):
        if module_name == "app" or module_name.startswith("app."):
            del sys.modules[module_name]
    sys.path.insert(0, str(service_path))
    try:
        module = importlib.import_module("app.main")
        return module.app
    finally:
        try:
            sys.path.remove(str(service_path))
        except ValueError:
            pass


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def sample_fixtures() -> dict[str, Any]:
    order_event_id = idempotent_event_id(
        tenant_id="tenant_demo",
        event_type="order.created",
        source_service="checkout-api",
        idempotency_key="checkout-ord-1001-created",
    )
    return {
        "ingestion_order_request.json": {
            "tenant_id": "tenant_demo",
            "event_type": "order.created",
            "source_service": "checkout-api",
            "idempotency_key": "checkout-ord-1001-created",
            "trace_id": "trace-demo-1001",
            "correlation_id": "corr-checkout-1001",
            "causation_id": None,
            "payload_version": 1,
            "payload": {
                "order_id": "ord_1001",
                "customer_id": "cust_1001",
                "product_id": "prod_001",
                "quantity": 2,
                "unit_price": 49.0,
                "discount_amount": 5.0,
                "currency": "USD",
                "status": "created",
                "channel": "web",
                "marketing_campaign_id": "paid-search-q2",
                "region": "na",
            },
        },
        "ingestion_order_response.json": {
            "event_id": order_event_id,
            "tenant_id": "tenant_demo",
            "event_type": "order.created",
            "topic": "platform.events.orders",
            "partition": 2,
            "offset": 1024,
            "trace_id": "trace-demo-1001",
            "correlation_id": "corr-checkout-1001",
            "causation_id": None,
            "idempotency_key": "checkout-ord-1001-created",
        },
        "analytics_revenue_response.json": {
            "tenant_id": "tenant_demo",
            "metric": "revenue",
            "cached": False,
            "count": 1,
            "data": [
                {
                    "metric_date": "2026-05-25",
                    "gross_revenue": 201884.0,
                    "net_revenue": 184230.75,
                    "order_count": 2187,
                    "units_sold": 3412,
                    "average_order_value": 84.24,
                }
            ],
        },
        "metadata_token_request.json": {
            "tenant_id": "tenant_demo",
            "user_id": "analyst_demo",
            "role": "analyst",
            "scopes": ["metrics:read"],
            "expires_in_seconds": 3600,
        },
    }


def export_contracts(output_dir: Path, fixtures_dir: Path) -> list[Path]:
    written: list[Path] = []
    for service_name in SERVICES:
        app = import_service_app(service_name)
        path = output_dir / f"{service_name}.openapi.json"
        write_json(path, app.openapi())
        written.append(path)
    for filename, payload in sample_fixtures().items():
        path = fixtures_dir / filename
        write_json(path, payload)
        written.append(path)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Export service OpenAPI JSON and sample fixtures.")
    parser.add_argument("--output-dir", default="api/openapi")
    parser.add_argument("--fixtures-dir", default="api/fixtures")
    args = parser.parse_args()
    written = export_contracts(Path(args.output_dir), Path(args.fixtures_dir))
    print(json.dumps({"written": [str(path) for path in written]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
