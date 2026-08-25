from __future__ import annotations

from pathlib import Path

from platform_shared.schemas import EventType, build_envelope, idempotent_event_id

from scripts.check_contract_compatibility import validate_cases
from scripts.compare_benchmarks import compare
from scripts.reconcile_metrics import evaluate_reconciliation
from scripts.reconciliation_summary import summarize_reconciliation
from scripts.validate_tenant_rls import validate_rls_sql, validation_queries

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_idempotent_event_id_is_stable_and_builds_replay_safe_envelope() -> None:
    event_id = idempotent_event_id(
        tenant_id="tenant_demo",
        event_type=EventType.ORDER_CREATED,
        source_service="checkout-api",
        idempotency_key="checkout-ord-1001-created",
    )
    repeated_event_id = idempotent_event_id(
        tenant_id="tenant_demo",
        event_type="order.created",
        source_service="checkout-api",
        idempotency_key="checkout-ord-1001-created",
    )
    envelope = build_envelope(
        event_id=event_id,
        tenant_id="tenant_demo",
        event_type=EventType.ORDER_CREATED,
        source_service="checkout-api",
        payload={
            "order_id": "ord_1001",
            "customer_id": "cust_1001",
            "product_id": "prod_001",
            "quantity": 1,
            "unit_price": 49.0,
        },
    )

    assert event_id == repeated_event_id
    assert event_id.startswith("idem_")
    assert envelope.event_id == event_id


def test_tenant_rls_validator_requires_policy_coverage_and_with_check() -> None:
    sql = (PROJECT_ROOT / "database" / "security" / "tenant_rls.sql").read_text()
    errors = validate_rls_sql(sql)
    queries = validation_queries("tenant_demo")

    assert errors == []
    assert "set_config('app.tenant_id', 'tenant_demo', true)" in queries[1]


def test_reconciliation_detects_metric_drift() -> None:
    rows = [
        {
            "tenant_id": "tenant_demo",
            "metric_date": "2026-05-01",
            "serving_net_revenue": 100.0,
            "recomputed_net_revenue": 100.0,
            "serving_order_count": 4,
            "recomputed_order_count": 4,
            "serving_units_sold": 7,
            "recomputed_units_sold": 7,
            "serving_events_processed": 20,
        },
        {
            "tenant_id": "tenant_demo",
            "metric_date": "2026-05-02",
            "serving_net_revenue": 97.0,
            "recomputed_net_revenue": 100.0,
            "serving_order_count": 3,
            "recomputed_order_count": 4,
            "serving_units_sold": 6,
            "recomputed_units_sold": 7,
            "serving_events_processed": 18,
        },
    ]

    results = evaluate_reconciliation(rows, revenue_tolerance=0.01)

    assert results[0]["status"] == "passed"
    assert results[1]["status"] == "failed"
    assert results[1]["revenue_delta"] == -3.0


def test_reconciliation_summary_rolls_up_failed_checks() -> None:
    summary = summarize_reconciliation(
        [
            {
                "tenant_id": "tenant_demo",
                "checked_date": "2026-05-01",
                "checks_run": 7,
                "passed_checks": 7,
                "failed_checks": 0,
            },
            {
                "tenant_id": "tenant_enterprise",
                "checked_date": "2026-05-01",
                "checks_run": 7,
                "passed_checks": 6,
                "failed_checks": 1,
            },
        ]
    )

    assert summary["status"] == "failed"
    assert summary["tenant_count"] == 2
    assert summary["checks_run"] == 14
    assert summary["failed_checks"] == 1


def test_contract_compatibility_case_allows_optional_v2_fields() -> None:
    errors = validate_cases(PROJECT_ROOT / "contracts" / "compatibility_tests" / "order_v1_to_v2.json")

    assert errors == []


def test_benchmark_compare_flags_throughput_regression() -> None:
    baseline = {
        "benchmark_name": "baseline",
        "events": 1000,
        "events_per_second": 100.0,
        "failure_rate": 0.0,
        "p95_latency_ms": 100.0,
    }
    current = {
        "benchmark_name": "current",
        "events": 1000,
        "events_per_second": 70.0,
        "failure_rate": 0.0,
        "p95_latency_ms": 110.0,
    }

    result = compare(current, baseline, min_throughput_ratio=0.8)

    assert result["status"] == "failed"
    assert result["throughput_ratio"] == 0.7
