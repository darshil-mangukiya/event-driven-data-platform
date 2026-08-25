"""Reconcile the Kafka-consumer serving path against the dbt
analytics-engineering path, per the contract in
`metrics/contracts/kafka_dbt_metric_reconciliation.json`.

The comparison applies to payment success and failure counts. dbt's
`fct_tenant_daily_metrics` model already computes a fresh,
independently-derived `observed_successful_payments`/
`observed_failed_payments` pair (via a direct COUNT over
`processed_payments`), sitting right next to the Kafka-consumer's own
incrementally-upserted `payment_success_count`/`payment_failure_count`
in the same row.

Usage:
    python scripts/reconcile_kafka_dbt_metrics.py \
        --database-url postgresql://platform:platform@localhost:15432/data_platform \
        --pretty
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import asyncpg

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONTRACT_PATH = PROJECT_ROOT / "metrics" / "contracts" / "kafka_dbt_metric_reconciliation.json"

# metric_name -> (serving_column, dbt_column)
RECONCILED_COLUMNS = {
    "payment_success_count": ("payment_success_count", "observed_successful_payments"),
    "payment_failure_count": ("payment_failure_count", "observed_failed_payments"),
}


def load_contract() -> dict[str, Any]:
    return json.loads(CONTRACT_PATH.read_text())


async def _fetch_serving_rows(conn: asyncpg.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    rows = await conn.fetch(
        "select tenant_id, metric_date, payment_success_count, payment_failure_count "
        "from tenant_metrics_daily"
    )
    return {(r["tenant_id"], r["metric_date"].isoformat()): dict(r) for r in rows}


async def _fetch_dbt_rows(conn: asyncpg.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    rows = await conn.fetch(
        "select tenant_id, metric_date, observed_successful_payments, observed_failed_payments "
        "from analytics_mart.fct_tenant_daily_metrics"
    )
    return {(r["tenant_id"], r["metric_date"].isoformat()): dict(r) for r in rows}


def _status_for(serving_value: int | None, dbt_value: int | None, tolerance: float) -> tuple[str, float, float]:
    if serving_value is None:
        return "MISSING_IN_SERVING", 0.0, 0.0
    if dbt_value is None:
        return "MISSING_IN_DBT", 0.0, 0.0
    abs_diff = abs(serving_value - dbt_value)
    rel_diff = (abs_diff / dbt_value) if dbt_value else (0.0 if abs_diff == 0 else float("inf"))
    if abs_diff <= tolerance:
        return ("MATCH" if abs_diff == 0 else "WITHIN_TOLERANCE"), abs_diff, rel_diff
    return "FAIL", abs_diff, rel_diff


async def run_reconciliation(database_url: str) -> dict[str, Any]:
    contract = load_contract()
    conn = await asyncpg.connect(database_url)
    try:
        serving = await _fetch_serving_rows(conn)
        dbt = await _fetch_dbt_rows(conn)
    finally:
        await conn.close()

    all_keys = sorted(set(serving) | set(dbt))
    rows: list[dict[str, Any]] = []

    for metric_contract in contract["reconciled_metrics"]:
        metric_name = metric_contract["metric_name"]
        serving_col, dbt_col = RECONCILED_COLUMNS[metric_name]
        tolerance = metric_contract["allowed_tolerance"]["value"]

        for tenant_id, metric_date in all_keys:
            serving_row = serving.get((tenant_id, metric_date))
            dbt_row = dbt.get((tenant_id, metric_date))
            serving_value = serving_row[serving_col] if serving_row else None
            dbt_value = dbt_row[dbt_col] if dbt_row else None
            status, abs_diff, rel_diff = _status_for(serving_value, dbt_value, tolerance)
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "metric_date": metric_date,
                    "metric_name": metric_name,
                    "serving_value": serving_value,
                    "dbt_value": dbt_value,
                    "absolute_difference": abs_diff,
                    "relative_difference": rel_diff,
                    "allowed_tolerance": tolerance,
                    "status": status,
                }
            )

    critical_failures = [r for r in rows if r["status"] == "FAIL"]
    missing = [r for r in rows if r["status"] in ("MISSING_IN_SERVING", "MISSING_IN_DBT")]
    overall = "FAIL" if critical_failures else "PASS"

    return {
        "overall_status": overall,
        "rows_checked": len(rows),
        "tenant_date_pairs_checked": len(all_keys),
        "critical_failures": len(critical_failures),
        "missing_rows": len(missing),
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile the Kafka-consumer and dbt metric paths.")
    parser.add_argument(
        "--database-url",
        default="postgresql://platform:platform@localhost:15432/data_platform",
    )
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    import asyncio

    report = asyncio.run(run_reconciliation(args.database_url))
    print(json.dumps(report, indent=2 if args.pretty else None, sort_keys=True))
    if report["overall_status"] == "FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
