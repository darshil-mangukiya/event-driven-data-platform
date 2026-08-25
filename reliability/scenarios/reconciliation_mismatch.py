"""Reconciliation-mismatch reliability exercise.

Feeds a deliberately mismatched (serving vs. recomputed) row through the
real ``scripts/reconcile_metrics.py::evaluate_reconciliation`` function and
proves the mismatch is detected, not averaged away or silently tolerated.
"""

from __future__ import annotations

from datetime import datetime, timezone

from reliability.injectors.reachability import postgres_reachable
from reliability.models import ScenarioResult, StepResult
from spark.streaming.config import StreamingConfig

SCENARIO_ID = "reconciliation-mismatch"


def run(config: StreamingConfig | None = None) -> ScenarioResult:
    from scripts.reconcile_metrics import evaluate_reconciliation

    config = config or StreamingConfig()
    steps: list[StepResult] = []

    matched_row = {
        "tenant_id": "tenant_demo",
        "metric_date": "2026-08-01",
        "serving_net_revenue": 1000.00,
        "recomputed_net_revenue": 1000.00,
        "serving_order_count": 10,
        "recomputed_order_count": 10,
        "serving_units_sold": 25,
        "recomputed_units_sold": 25,
        "serving_events_processed": 40,
    }
    mismatched_row = {
        "tenant_id": "tenant_demo",
        "metric_date": "2026-08-02",
        "serving_net_revenue": 1000.00,
        "recomputed_net_revenue": 850.00,  # deliberate $150 gap — beyond tolerance
        "serving_order_count": 12,
        "recomputed_order_count": 10,  # deliberate 2-order gap
        "serving_units_sold": 30,
        "recomputed_units_sold": 30,
        "serving_events_processed": 45,
    }

    results = evaluate_reconciliation([matched_row, mismatched_row], revenue_tolerance=0.01)
    by_date = {row["metric_date"]: row for row in results}

    ok = (
        by_date["2026-08-01"]["status"] == "passed"
        and by_date["2026-08-02"]["status"] == "failed"
        and by_date["2026-08-02"]["revenue_delta"] == 150.00
        and by_date["2026-08-02"]["order_count_delta"] == 2
    )
    steps.append(
        StepResult(
            name="detect_mismatch_with_real_reconciliation_logic",
            status="verified" if ok else "failed",
            detail=(
                "evaluate_reconciliation correctly passed the matched row and flagged the mismatched "
                "row (revenue_delta=150.00, order_count_delta=2) as failed"
                if ok
                else f"unexpected reconciliation results: {results}"
            ),
            evidence={"results": results},
        )
    )

    db_up = postgres_reachable("postgresql://platform:platform@localhost:5432/data_platform")
    if db_up:
        steps.append(
            StepResult(
                name="live_reconciliation_against_database",
                status="not_run",
                detail=(
                    "PostgreSQL is reachable, but this exercise deliberately does not write a fake "
                    "mismatch into live serving tables (would corrupt real demo data) — run "
                    "`make reconciliation-dry-run` or `platform_cli ops reconciliation` against real "
                    "data for a live check instead."
                ),
            )
        )
    else:
        steps.append(
            StepResult(
                name="live_reconciliation_against_database",
                status="not_run",
                detail="PostgreSQL not reachable in this environment",
            )
        )

    result = ScenarioResult(
        scenario_id=SCENARIO_ID,
        title="Reconciliation Mismatch",
        component="scripts.reconcile_metrics.evaluate_reconciliation / reconciliation_audit table",
        expected_behavior=(
            "When a tenant's serving-layer daily metrics diverge from the recomputed source-of-truth "
            "numbers beyond the configured tolerance, reconciliation must flag it as 'failed' with the "
            "exact deltas — not silently pass, not average it into a fleet-wide number."
        ),
        detection_method="evaluate_reconciliation() sets status='failed' when abs(revenue_delta) > tolerance or order/units deltas are non-zero; persisted to reconciliation_audit.",
        impact="A silent reconciliation gap would let a real serving-layer bug (e.g. a double-processed batch, a dropped Kafka partition) go unnoticed indefinitely.",
        root_cause="This exercise's injected mismatch represents causes such as duplicate processing, a partial or failed batch write, or a schema or unit mismatch between serving and source tables.",
        recovery="Re-run the affected date range through scripts/backfill_metrics.py once the root cause is fixed, then re-reconcile to confirm the delta closes to zero.",
        corrective_action="Investigate the specific tenant/date's processed_orders vs tenant_metrics_daily rows to find the divergence's origin (see docs/reliability.md's reconciliation runbook).",
        preventive_control="make reconciliation-dry-run / platform_cli ops reconciliation are safe to run on a schedule; reconciliation_audit gives a persistent, queryable history rather than a one-off manual check.",
        steps=steps,
    )
    result.ended_at = datetime.now(timezone.utc)
    return result
