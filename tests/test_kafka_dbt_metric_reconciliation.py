"""Tests for scripts/reconcile_kafka_dbt_metrics.py — reconciliation between the Kafka-consumer
serving path (tenant_metrics_daily) and the dbt analytics-engineering
path (analytics_mart.fct_tenant_daily_metrics's independently-derived
observed_successful_payments/observed_failed_payments columns).

The live-database integration path (run_reconciliation against a real
Postgres) is covered by
evidence/validation/kafka-dbt-metric-reconciliation.md, not duplicated
here as a skip-cleanly integration test — these tests exercise the pure
comparison/status logic and the row-assembly logic deterministically and
offline, matching this project's established pattern (see
tests/test_oidc.py's wrong-audience test for the same offline-first
preference).
"""

from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch

from scripts.reconcile_kafka_dbt_metrics import _status_for, run_reconciliation


def test_status_for_exact_match() -> None:
    status, abs_diff, rel_diff = _status_for(100, 100, tolerance=0)
    assert status == "MATCH"
    assert abs_diff == 0
    assert rel_diff == 0.0


def test_status_for_within_tolerance() -> None:
    status, abs_diff, rel_diff = _status_for(101, 100, tolerance=2)
    assert status == "WITHIN_TOLERANCE"
    assert abs_diff == 1


def test_status_for_outside_tolerance_fails() -> None:
    status, abs_diff, rel_diff = _status_for(150, 100, tolerance=2)
    assert status == "FAIL"
    assert abs_diff == 50
    assert rel_diff == 0.5


def test_status_for_zero_denominator_does_not_raise() -> None:
    # dbt_value == 0 with a real difference must not divide-by-zero.
    status, abs_diff, rel_diff = _status_for(5, 0, tolerance=0)
    assert status == "FAIL"
    assert abs_diff == 5
    assert rel_diff == float("inf")


def test_status_for_zero_both_sides_is_a_clean_match() -> None:
    status, abs_diff, rel_diff = _status_for(0, 0, tolerance=0)
    assert status == "MATCH"
    assert rel_diff == 0.0


def test_status_for_missing_in_serving() -> None:
    status, _, _ = _status_for(None, 42, tolerance=0)
    assert status == "MISSING_IN_SERVING"


def test_status_for_missing_in_dbt() -> None:
    status, _, _ = _status_for(42, None, tolerance=0)
    assert status == "MISSING_IN_DBT"


def _fake_asyncpg_connect(serving_rows: list[dict], dbt_rows: list[dict]):
    """Build a mock asyncpg.connect() whose .fetch() returns serving_rows
    for the tenant_metrics_daily query and dbt_rows for the
    analytics_mart.fct_tenant_daily_metrics query, keyed on which SQL
    string was passed (matching how the real queries are distinguished).
    """

    async def fake_fetch(sql: str, *args):
        if "analytics_mart" in sql:
            return [dict(r) for r in dbt_rows]
        return [dict(r) for r in serving_rows]

    conn = AsyncMock()
    conn.fetch = AsyncMock(side_effect=fake_fetch)
    conn.close = AsyncMock()

    async def fake_connect(*args, **kwargs):
        return conn

    return fake_connect


def test_run_reconciliation_reports_missing_tenant_date_pair() -> None:
    # A tenant/date present in serving but never built by dbt.
    serving = [{"tenant_id": "tenant_a", "metric_date": date(2026, 1, 1), "payment_success_count": 10, "payment_failure_count": 1}]
    dbt: list[dict] = []

    with patch("asyncpg.connect", new=_fake_asyncpg_connect(serving, dbt)):
        report = asyncio.run(run_reconciliation("postgresql://fake/fake"))

    assert report["missing_rows"] == 2  # one row per reconciled metric (success + failure counts)
    assert all(r["status"] == "MISSING_IN_DBT" for r in report["rows"])
    assert report["overall_status"] == "PASS"  # missing rows are not the same as a numeric mismatch


def test_run_reconciliation_flags_a_real_mismatch_as_fail() -> None:
    serving = [{"tenant_id": "tenant_a", "metric_date": date(2026, 1, 1), "payment_success_count": 10, "payment_failure_count": 1}]
    dbt = [{"tenant_id": "tenant_a", "metric_date": date(2026, 1, 1), "observed_successful_payments": 7, "observed_failed_payments": 1}]

    with patch("asyncpg.connect", new=_fake_asyncpg_connect(serving, dbt)):
        report = asyncio.run(run_reconciliation("postgresql://fake/fake"))

    assert report["overall_status"] == "FAIL"
    assert report["critical_failures"] == 1
    success_row = next(r for r in report["rows"] if r["metric_name"] == "payment_success_count")
    assert success_row["status"] == "FAIL"
    assert success_row["absolute_difference"] == 3
    failure_row = next(r for r in report["rows"] if r["metric_name"] == "payment_failure_count")
    assert failure_row["status"] == "MATCH"


def test_run_reconciliation_never_rewrites_values() -> None:
    # A structural guarantee, beyond a behavioral one: run_reconciliation
    # has no write/UPDATE path at all — confirmed by the mock connection
    # never receiving an .execute() call.
    serving = [{"tenant_id": "tenant_a", "metric_date": date(2026, 1, 1), "payment_success_count": 5, "payment_failure_count": 0}]
    dbt = [{"tenant_id": "tenant_a", "metric_date": date(2026, 1, 1), "observed_successful_payments": 3, "observed_failed_payments": 0}]

    async def fake_fetch(sql: str, *args):
        if "analytics_mart" in sql:
            return [dict(r) for r in dbt]
        return [dict(r) for r in serving]

    conn = AsyncMock()
    conn.fetch = AsyncMock(side_effect=fake_fetch)
    conn.close = AsyncMock()

    async def fake_connect(*args, **kwargs):
        return conn

    with patch("asyncpg.connect", new=fake_connect):
        asyncio.run(run_reconciliation("postgresql://fake/fake"))

    conn.execute.assert_not_called()


def test_run_reconciliation_isolates_multiple_tenants_independently() -> None:
    serving = [
        {"tenant_id": "tenant_a", "metric_date": date(2026, 1, 1), "payment_success_count": 10, "payment_failure_count": 2},
        {"tenant_id": "tenant_b", "metric_date": date(2026, 1, 1), "payment_success_count": 20, "payment_failure_count": 4},
    ]
    dbt = [
        {"tenant_id": "tenant_a", "metric_date": date(2026, 1, 1), "observed_successful_payments": 10, "observed_failed_payments": 2},
        # tenant_b's dbt row is wrong on purpose — must not contaminate tenant_a's result.
        {"tenant_id": "tenant_b", "metric_date": date(2026, 1, 1), "observed_successful_payments": 999, "observed_failed_payments": 4},
    ]

    with patch("asyncpg.connect", new=_fake_asyncpg_connect(serving, dbt)):
        report = asyncio.run(run_reconciliation("postgresql://fake/fake"))

    tenant_a_rows = [r for r in report["rows"] if r["tenant_id"] == "tenant_a"]
    tenant_b_rows = [r for r in report["rows"] if r["tenant_id"] == "tenant_b"]
    assert all(r["status"] == "MATCH" for r in tenant_a_rows)
    assert any(r["status"] == "FAIL" for r in tenant_b_rows)
    assert report["overall_status"] == "FAIL"


def test_contract_file_is_valid_and_declares_exactly_the_reconciled_metrics() -> None:
    from scripts.reconcile_kafka_dbt_metrics import RECONCILED_COLUMNS, load_contract

    contract = load_contract()
    declared = {m["metric_name"] for m in contract["reconciled_metrics"]}
    assert declared == set(RECONCILED_COLUMNS)
    assert "payment_success_count" in declared
    assert "payment_failure_count" in declared
    # The passthrough fields must be explicitly documented as
    # NOT_APPLICABLE, not silently absent from the contract.
    passthrough_names = {m["metric_name"] for m in contract["passthrough_not_reconciled"]}
    assert {"net_revenue", "gross_revenue", "order_count", "active_users"}.issubset(passthrough_names)
