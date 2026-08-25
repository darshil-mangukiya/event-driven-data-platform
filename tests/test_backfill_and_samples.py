from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from scripts.backfill_metrics import (
    BackfillRequest,
    build_backfill_plan,
    build_daily_metrics_backfill_sql,
    validate_request,
)
from scripts.validate_sample_artifacts import validate_samples

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_backfill_plan_is_tenant_scoped_and_idempotent() -> None:
    request = BackfillRequest(
        tenant_id="tenant_demo",
        start_date=date(2026, 5, 1),
        end_date=date(2026, 5, 7),
        requested_by="unit-test",
        dry_run=True,
    )

    plan = build_backfill_plan(request)
    sql = build_daily_metrics_backfill_sql()

    assert plan["tenant_id"] == "tenant_demo"
    assert plan["idempotency"] == "delete_and_recompute_tenant_date_range"
    assert [statement["name"] for statement in plan["statements"]] == [
        "delete_existing_daily_metrics",
        "insert_recomputed_daily_metrics",
    ]
    assert "where tenant_id = $1" in sql
    assert "insert into tenant_metrics_daily" in sql


def test_backfill_rejects_unbounded_or_large_routine_windows() -> None:
    bad_order = BackfillRequest(
        tenant_id="tenant_demo",
        start_date=date(2026, 5, 7),
        end_date=date(2026, 5, 1),
        requested_by="unit-test",
    )
    with pytest.raises(ValueError, match="start-date"):
        validate_request(bad_order)

    large_window = BackfillRequest(
        tenant_id="tenant_demo",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 15),
        requested_by="unit-test",
        max_window_days=31,
    )
    with pytest.raises(ValueError, match="date window"):
        validate_request(large_window)


def test_sample_artifacts_validate_against_contracts() -> None:
    errors = validate_samples(PROJECT_ROOT / "samples")

    assert errors == []
