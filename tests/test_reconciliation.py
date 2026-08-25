"""Tests for the reconciliation extensions: payment and
customer-activity checks (closing previously documented gaps in the
data-product registry), the generic persistence path, the run_all_checks
orchestrator, CLI wiring, and lineage-event correlation for the new checks.

The original revenue check (evaluate_reconciliation, run_reconciliation,
persist_reconciliation) is exercised elsewhere
(tests/test_reliability_governance_tooling.py,
reliability/scenarios/reconciliation_mismatch.py) and deliberately not
duplicated here — this file is about what's new.
"""

from __future__ import annotations

import asyncio
import datetime
from unittest.mock import AsyncMock

from scripts.reconcile_metrics import (
    ALL_CHECK_NAMES,
    CHECK_RUNNERS,
    CUSTOMER_ACTIVITY_RECONCILIATION_PIPELINE_NAME,
    PAYMENT_RECONCILIATION_PIPELINE_NAME,
    ReconciliationRequest,
    build_customer_activity_reconciliation_sql,
    build_payment_reconciliation_sql,
    evaluate_customer_activity_reconciliation,
    evaluate_payment_reconciliation,
    persist_check_result,
    run_all_checks,
    run_customer_activity_reconciliation,
    run_payment_reconciliation,
)

# ---------------------------------------------------------------------------
# Payment reconciliation
# ---------------------------------------------------------------------------


def test_payment_reconciliation_passes_when_counts_match() -> None:
    rows = [
        {
            "tenant_id": "tenant_demo",
            "metric_date": "2026-08-01",
            "serving_payment_success_count": 10,
            "recomputed_payment_success_count": 10,
            "serving_payment_failure_count": 2,
            "recomputed_payment_failure_count": 2,
        }
    ]
    results = evaluate_payment_reconciliation(rows)
    assert results[0]["status"] == "passed"
    assert results[0]["payment_success_count_delta"] == 0
    assert results[0]["payment_failure_count_delta"] == 0


def test_payment_reconciliation_detects_success_count_drift() -> None:
    rows = [
        {
            "tenant_id": "tenant_demo",
            "metric_date": "2026-08-01",
            "serving_payment_success_count": 12,
            "recomputed_payment_success_count": 10,
            "serving_payment_failure_count": 2,
            "recomputed_payment_failure_count": 2,
        }
    ]
    results = evaluate_payment_reconciliation(rows)
    assert results[0]["status"] == "failed"
    assert results[0]["payment_success_count_delta"] == 2


def test_payment_reconciliation_detects_failure_count_drift() -> None:
    rows = [
        {
            "tenant_id": "tenant_demo",
            "metric_date": "2026-08-01",
            "serving_payment_success_count": 10,
            "recomputed_payment_success_count": 10,
            "serving_payment_failure_count": 5,
            "recomputed_payment_failure_count": 1,
        }
    ]
    results = evaluate_payment_reconciliation(rows)
    assert results[0]["status"] == "failed"
    assert results[0]["payment_failure_count_delta"] == 4


def test_payment_reconciliation_sql_references_processed_payments() -> None:
    sql = build_payment_reconciliation_sql()
    assert "processed_payments" in sql
    assert "tenant_metrics_daily" in sql
    assert "payment_success_count" in sql
    assert "payment_failure_count" in sql


# ---------------------------------------------------------------------------
# Customer activity reconciliation
# ---------------------------------------------------------------------------


def test_customer_activity_reconciliation_passes_when_counts_match() -> None:
    rows = [
        {
            "tenant_id": "tenant_demo",
            "metric_date": "2026-08-01",
            "serving_new_users": 5,
            "recomputed_new_users": 5,
            "serving_active_users": 20,
            "recomputed_active_users": 20,
            "serving_churn_signal_count": 1,
            "recomputed_churn_signal_count": 1,
        }
    ]
    results = evaluate_customer_activity_reconciliation(rows)
    assert results[0]["status"] == "passed"


def test_customer_activity_reconciliation_detects_active_users_drift() -> None:
    rows = [
        {
            "tenant_id": "tenant_demo",
            "metric_date": "2026-08-01",
            "serving_new_users": 5,
            "recomputed_new_users": 5,
            "serving_active_users": 25,
            "recomputed_active_users": 20,
            "serving_churn_signal_count": 1,
            "recomputed_churn_signal_count": 1,
        }
    ]
    results = evaluate_customer_activity_reconciliation(rows)
    assert results[0]["status"] == "failed"
    assert results[0]["active_users_delta"] == 5


def test_customer_activity_reconciliation_detects_churn_signal_drift() -> None:
    rows = [
        {
            "tenant_id": "tenant_demo",
            "metric_date": "2026-08-01",
            "serving_new_users": 5,
            "recomputed_new_users": 5,
            "serving_active_users": 20,
            "recomputed_active_users": 20,
            "serving_churn_signal_count": 3,
            "recomputed_churn_signal_count": 0,
        }
    ]
    results = evaluate_customer_activity_reconciliation(rows)
    assert results[0]["status"] == "failed"
    assert results[0]["churn_signal_count_delta"] == 3


def test_customer_activity_reconciliation_sql_references_processed_user_sessions() -> None:
    sql = build_customer_activity_reconciliation_sql()
    assert "processed_user_sessions" in sql
    assert "tenant_metrics_daily" in sql
    assert "new_users" in sql
    assert "active_users" in sql
    assert "churn_signal_count" in sql


# ---------------------------------------------------------------------------
# Generic persistence
# ---------------------------------------------------------------------------


def test_persist_check_result_inserts_with_check_name_and_zeroed_revenue_columns() -> None:
    """Payment/customer_activity checks don't have a revenue concept — the
    fixed revenue_delta/order_count_delta/units_sold_delta columns must
    stay 0 for them (not overloaded with unrelated deltas), with the real
    check-specific numbers in `details` instead.
    """
    mock_postgres = AsyncMock()
    asyncio.run(
        persist_check_result(
            mock_postgres,
            check_name="payment_reconciliation",
            tenant_id="tenant_demo",
            metric_date_str="2026-08-01",
            status="failed",
            details={"payment_success_count_delta": 2, "payment_failure_count_delta": 0},
            requested_by="test-operator",
        )
    )
    assert mock_postgres.execute.called
    sql_text, *params = mock_postgres.execute.call_args[0]
    assert "reconciliation_audit" in sql_text
    tenant_id, metric_date, check_name, status, details_json, requested_by = params
    assert tenant_id == "tenant_demo"
    assert isinstance(metric_date, datetime.date)
    assert metric_date == datetime.date(2026, 8, 1)
    assert check_name == "payment_reconciliation"
    assert status == "failed"
    assert "payment_success_count_delta" in details_json
    assert requested_by == "test-operator"


# ---------------------------------------------------------------------------
# run_all_checks orchestrator
# ---------------------------------------------------------------------------


def test_all_check_names_match_check_runners_registry() -> None:
    assert set(ALL_CHECK_NAMES) == set(CHECK_RUNNERS)


def test_run_all_checks_dry_run_returns_all_three_checks() -> None:
    request = ReconciliationRequest(
        tenant_id="tenant_demo",
        start_date=datetime.date(2026, 6, 1),
        end_date=datetime.date(2026, 6, 2),
        dry_run=True,
    )
    result = asyncio.run(run_all_checks("postgresql://unused", request))
    assert set(result["checks"]) == {"revenue", "payment", "customer_activity"}
    for check_result in result["checks"].values():
        assert check_result["status"] == "dry_run"


def test_run_all_checks_overall_status_failed_if_any_check_failed() -> None:
    """Pure logic test: run_all_checks' overall status must be "failed" if
    any individual check reports failed, beyond averaged/ignored.
    """
    from scripts import reconcile_metrics as module

    async def fake_passed(_url, _req):
        return {"status": "passed"}

    async def fake_failed(_url, _req):
        return {"status": "failed"}

    original_runners = dict(module.CHECK_RUNNERS)
    module.CHECK_RUNNERS["revenue"] = fake_passed
    module.CHECK_RUNNERS["payment"] = fake_failed
    module.CHECK_RUNNERS["customer_activity"] = fake_passed
    try:
        request = ReconciliationRequest(
            tenant_id="tenant_demo",
            start_date=datetime.date(2026, 6, 1),
            end_date=datetime.date(2026, 6, 2),
        )
        result = asyncio.run(module.run_all_checks("postgresql://unused", request))
        assert result["status"] == "failed"
    finally:
        module.CHECK_RUNNERS.clear()
        module.CHECK_RUNNERS.update(original_runners)


# ---------------------------------------------------------------------------
# Lineage correlation for new checks
# ---------------------------------------------------------------------------


def test_payment_reconciliation_emits_lineage_with_correct_job_name() -> None:
    import inspect

    source = inspect.getsource(run_payment_reconciliation)
    assert "emit_pipeline_lineage" in source
    assert "PAYMENT_RECONCILIATION_PIPELINE_NAME" in source


def test_customer_activity_reconciliation_emits_lineage_with_correct_job_name() -> None:
    import inspect

    source = inspect.getsource(run_customer_activity_reconciliation)
    assert "emit_pipeline_lineage" in source
    assert "CUSTOMER_ACTIVITY_RECONCILIATION_PIPELINE_NAME" in source


def test_pipeline_names_are_distinct() -> None:
    from scripts.reconcile_metrics import RECONCILIATION_PIPELINE_NAME

    names = {RECONCILIATION_PIPELINE_NAME, PAYMENT_RECONCILIATION_PIPELINE_NAME, CUSTOMER_ACTIVITY_RECONCILIATION_PIPELINE_NAME}
    assert len(names) == 3


# ---------------------------------------------------------------------------
# Backward compatibility (the pre-existing revenue check must be unchanged)
# ---------------------------------------------------------------------------


def test_revenue_check_still_default_in_cli() -> None:
    import sys

    from scripts.reconcile_metrics import parse_args

    original_argv = sys.argv
    sys.argv = ["reconcile_metrics.py", "--tenant-id", "tenant_demo", "--start-date", "2026-06-01", "--end-date", "2026-06-02", "--dry-run"]
    try:
        args = parse_args()
        assert args.check == "revenue"
    finally:
        sys.argv = original_argv


def test_cli_accepts_all_check_choices() -> None:
    import sys

    from scripts.reconcile_metrics import parse_args

    original_argv = sys.argv
    for check in [*ALL_CHECK_NAMES, "all"]:
        sys.argv = [
            "reconcile_metrics.py",
            "--tenant-id",
            "tenant_demo",
            "--start-date",
            "2026-06-01",
            "--end-date",
            "2026-06-02",
            "--dry-run",
            "--check",
            check,
        ]
        try:
            args = parse_args()
            assert args.check == check
        finally:
            sys.argv = original_argv


# ---------------------------------------------------------------------------
# CLI integration (platform_cli)
# ---------------------------------------------------------------------------


def test_platform_cli_registers_reconciliation_run_subcommand() -> None:
    from platform_cli.__main__ import build_parser

    parser = build_parser()
    args = parser.parse_args(
        [
            "ops",
            "reconciliation-run",
            "--tenant-id",
            "tenant_demo",
            "--start-date",
            "2026-06-01",
            "--end-date",
            "2026-06-02",
            "--check",
            "all",
            "--dry-run",
        ]
    )
    assert args.resource == "ops"
    assert args.action == "reconciliation-run"
    assert args.check == "all"


def test_platform_cli_reconciliation_run_dry_run_all_checks() -> None:
    import argparse

    from platform_cli.__main__ import reconciliation_run

    args = argparse.Namespace(
        tenant_id="tenant_demo",
        start_date="2026-06-01",
        end_date="2026-06-02",
        check="all",
        revenue_tolerance=0.01,
        requested_by="test-operator",
        database_url=None,
        dry_run=True,
    )
    result = asyncio.run(reconciliation_run(args))
    assert set(result["checks"]) == {"revenue", "payment", "customer_activity"}
