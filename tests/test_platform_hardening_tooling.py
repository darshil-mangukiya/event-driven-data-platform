from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx

from scripts.emit_lineage_event import build_lineage_event
from scripts.incident_drill import drill_plan, load_incidents
from scripts.outbox_dispatch_plan import dispatch_plan
from scripts.schema_drift_report import drift_report
from scripts.validate_privacy_catalog import validate_privacy_catalog

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_producer_sdk_derives_stable_idempotent_request_and_publishes() -> None:
    sdk_path = PROJECT_ROOT / "sdk" / "python"
    if str(sdk_path) not in sys.path:
        sys.path.insert(0, str(sdk_path))
    from platform_producer import (
        PlatformProducerClient,
        ProducerEvent,
        derive_business_idempotency_key,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        return httpx.Response(
            202,
            json={
                "event_id": payload["event_id"],
                "tenant_id": payload["tenant_id"],
                "event_type": payload["event_type"],
                "topic": "platform.events.orders",
                "partition": 0,
                "offset": 1,
                "trace_id": payload.get("trace_id") or "trace-test",
                "idempotency_key": payload["idempotency_key"],
            },
        )

    client = PlatformProducerClient(
        base_url="http://ingestion.test",
        tenant_id="tenant_demo",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    event = ProducerEvent(
        tenant_id="tenant_demo",
        event_type="order.created",
        source_service="checkout-api",
        idempotency_key=derive_business_idempotency_key(
            source="checkout-api",
            entity_id="ord_1",
            action="created",
        ),
        payload={
            "order_id": "ord_1",
            "customer_id": "cust_1",
            "product_id": "prod_1",
            "quantity": 1,
            "unit_price": 10.0,
        },
    )

    response = client.publish(event)

    assert response["event_id"].startswith("idem_")
    assert response["idempotency_key"] == "checkout-api:ord_1:created"


def test_outbox_dispatch_plan_documents_lease_publish_retry_steps() -> None:
    plan = dispatch_plan(batch_size=50, retry_delay_seconds=15, max_attempts=3)

    assert [step["name"] for step in plan["steps"]] == [
        "lease_pending_events",
        "publish_to_kafka",
        "mark_published",
        "mark_failed_or_retry",
    ]
    assert "for update skip locked" in plan["steps"][0]["sql"].lower()


def test_lineage_event_shape_matches_openlineage_style() -> None:
    event = build_lineage_event(
        job_name="backfill_tenant_metrics_daily",
        run_id="run-1",
        tenant_id="tenant_demo",
        inputs=["processed_orders"],
        outputs=["tenant_metrics_daily"],
        status="succeeded",
    )

    assert event["eventType"] == "COMPLETE"
    assert event["job"]["namespace"] == "data-platform-system"
    assert event["inputs"][0]["name"] == "processed_orders"


def test_incident_drill_computes_burn_rate_and_postmortem_requirement() -> None:
    incidents = load_incidents(PROJECT_ROOT / "incidents" / "scenarios.json")
    plan = drill_plan(incidents[0])

    assert plan["incident_id"] == "inc_kafka_lag_spike"
    assert plan["postmortem_required"] is True
    assert plan["slo_burn_rate"] > 0


def test_privacy_catalog_validates_pii_handling() -> None:
    catalog = json.loads((PROJECT_ROOT / "governance" / "pii_classification.json").read_text())

    assert validate_privacy_catalog(catalog) == []


def test_schema_drift_report_passes_for_cataloged_tables() -> None:
    report = drift_report(
        (PROJECT_ROOT / "database" / "init" / "001_schema.sql").read_text(),
        json.loads((PROJECT_ROOT / "catalog" / "data_catalog.json").read_text()),
        PROJECT_ROOT / "database" / "migrations",
    )

    assert report["status"] == "passed"
    assert report["latest_revision"] == "0007_schema_registry"
    assert report["unmanaged_catalog_tables"] == []
