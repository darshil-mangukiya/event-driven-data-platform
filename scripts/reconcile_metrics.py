from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

from platform_shared.database import Postgres

from lineage.events import emit_pipeline_lineage

RECONCILIATION_PIPELINE_NAME = "reconciliation:tenant_metrics_daily"
RECONCILIATION_SOURCE_TABLES = ["tenant_metrics_daily", "processed_orders"]
RECONCILIATION_OUTPUT_TABLES = ["reconciliation_audit"]

# Two additional checks close gaps previously documented as
# open in contracts/data_products/registry.yml's payment_health and
# customer_activity products ("no dedicated reconciliation script yet").
# Each is a self-contained (build_sql, evaluate, persist, run) group,
# deliberately not sharing evaluate_reconciliation's exact revenue-shaped
# output — the fixed revenue_delta/order_count_delta/units_sold_delta
# columns on reconciliation_audit stay meaningful for the revenue check
# only; every other check's specifics live in the always-present `details`
# jsonb column instead of overloading those columns with unrelated deltas.
PAYMENT_RECONCILIATION_PIPELINE_NAME = "reconciliation:payment_health"
PAYMENT_RECONCILIATION_SOURCE_TABLES = ["tenant_metrics_daily", "processed_payments"]
PAYMENT_RECONCILIATION_OUTPUT_TABLES = ["reconciliation_audit"]

CUSTOMER_ACTIVITY_RECONCILIATION_PIPELINE_NAME = "reconciliation:customer_activity"
CUSTOMER_ACTIVITY_RECONCILIATION_SOURCE_TABLES = ["tenant_metrics_daily", "processed_user_sessions"]
CUSTOMER_ACTIVITY_RECONCILIATION_OUTPUT_TABLES = ["reconciliation_audit"]

ALL_CHECK_NAMES = ("revenue", "payment", "customer_activity")


@dataclass(frozen=True)
class ReconciliationRequest:
    tenant_id: str
    start_date: date
    end_date: date
    revenue_tolerance: float = 0.01
    requested_by: str = "local-operator"
    dry_run: bool = False


def build_daily_metrics_reconciliation_sql() -> str:
    return """
    with dates as (
        select generate_series($2::date, $3::date, interval '1 day')::date as metric_date
    ),
    recomputed as (
        select
            d.metric_date,
            coalesce(sum(o.net_revenue), 0) as recomputed_net_revenue,
            coalesce(count(o.order_id), 0) as recomputed_order_count,
            coalesce(sum(o.quantity), 0) as recomputed_units_sold
        from dates d
        left join processed_orders o
          on o.tenant_id = $1
         and o.event_timestamp::date = d.metric_date
        group by 1
    )
    select
        $1 as tenant_id,
        r.metric_date,
        coalesce(m.net_revenue, 0) as serving_net_revenue,
        r.recomputed_net_revenue,
        coalesce(m.order_count, 0) as serving_order_count,
        r.recomputed_order_count,
        coalesce(m.units_sold, 0) as serving_units_sold,
        r.recomputed_units_sold,
        coalesce(m.events_processed, 0) as serving_events_processed
    from recomputed r
    left join tenant_metrics_daily m
      on m.tenant_id = $1
     and m.metric_date = r.metric_date
    order by r.metric_date;
    """


def validate_request(request: ReconciliationRequest) -> None:
    if request.start_date > request.end_date:
        raise ValueError("start-date must be before or equal to end-date")


def evaluate_reconciliation(
    rows: list[dict[str, Any]],
    *,
    revenue_tolerance: float,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for row in rows:
        serving_revenue = float(row["serving_net_revenue"])
        recomputed_revenue = float(row["recomputed_net_revenue"])
        revenue_delta = round(serving_revenue - recomputed_revenue, 4)
        order_delta = int(row["serving_order_count"]) - int(row["recomputed_order_count"])
        units_delta = int(row["serving_units_sold"]) - int(row["recomputed_units_sold"])
        status = "passed"
        if abs(revenue_delta) > revenue_tolerance or order_delta != 0 or units_delta != 0:
            status = "failed"
        results.append(
            {
                "tenant_id": row["tenant_id"],
                "metric_date": str(row["metric_date"]),
                "status": status,
                "revenue_delta": revenue_delta,
                "order_count_delta": order_delta,
                "units_sold_delta": units_delta,
                "serving_events_processed": int(row["serving_events_processed"]),
            }
        )
    return results


def build_payment_reconciliation_sql() -> str:
    """Closes a gap explicitly documented as open in
    contracts/data_products/registry.yml's payment_health product
    ('no dedicated reconciliation script yet'). Recomputes payment
    success/failure counts straight from processed_payments and compares
    against tenant_metrics_daily's payment_success_count/payment_failure_count.
    """
    return """
    with dates as (
        select generate_series($2::date, $3::date, interval '1 day')::date as metric_date
    ),
    recomputed as (
        select
            d.metric_date,
            coalesce(count(*) filter (where p.status in ('authorized', 'captured')), 0) as recomputed_payment_success_count,
            coalesce(count(*) filter (where p.status = 'failed'), 0) as recomputed_payment_failure_count
        from dates d
        left join processed_payments p
          on p.tenant_id = $1
         and p.event_timestamp::date = d.metric_date
        group by 1
    )
    select
        $1 as tenant_id,
        r.metric_date,
        coalesce(m.payment_success_count, 0) as serving_payment_success_count,
        r.recomputed_payment_success_count,
        coalesce(m.payment_failure_count, 0) as serving_payment_failure_count,
        r.recomputed_payment_failure_count
    from recomputed r
    left join tenant_metrics_daily m
      on m.tenant_id = $1
     and m.metric_date = r.metric_date
    order by r.metric_date;
    """


def evaluate_payment_reconciliation(
    rows: list[dict[str, Any]],
    *,
    count_tolerance: int = 0,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for row in rows:
        success_delta = int(row["serving_payment_success_count"]) - int(row["recomputed_payment_success_count"])
        failure_delta = int(row["serving_payment_failure_count"]) - int(row["recomputed_payment_failure_count"])
        status = "passed"
        if abs(success_delta) > count_tolerance or abs(failure_delta) > count_tolerance:
            status = "failed"
        results.append(
            {
                "tenant_id": row["tenant_id"],
                "metric_date": str(row["metric_date"]),
                "status": status,
                "payment_success_count_delta": success_delta,
                "payment_failure_count_delta": failure_delta,
            }
        )
    return results


def build_customer_activity_reconciliation_sql() -> str:
    """Closes the same class of documented gap for the customer_activity
    product (contracts/data_products/registry.yml's reconciliation_rules
    was empty for this product previously).
    """
    return """
    with dates as (
        select generate_series($2::date, $3::date, interval '1 day')::date as metric_date
    ),
    recomputed as (
        select
            d.metric_date,
            coalesce(count(distinct u.user_id) filter (where u.action = 'signed_up'), 0) as recomputed_new_users,
            coalesce(count(distinct u.user_id), 0) as recomputed_active_users,
            coalesce(count(*) filter (where u.action in ('churn_signal', 'cancel_intent')), 0) as recomputed_churn_signal_count
        from dates d
        left join processed_user_sessions u
          on u.tenant_id = $1
         and u.event_timestamp::date = d.metric_date
        group by 1
    )
    select
        $1 as tenant_id,
        r.metric_date,
        coalesce(m.new_users, 0) as serving_new_users,
        r.recomputed_new_users,
        coalesce(m.active_users, 0) as serving_active_users,
        r.recomputed_active_users,
        coalesce(m.churn_signal_count, 0) as serving_churn_signal_count,
        r.recomputed_churn_signal_count
    from recomputed r
    left join tenant_metrics_daily m
      on m.tenant_id = $1
     and m.metric_date = r.metric_date
    order by r.metric_date;
    """


def evaluate_customer_activity_reconciliation(
    rows: list[dict[str, Any]],
    *,
    count_tolerance: int = 0,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for row in rows:
        new_users_delta = int(row["serving_new_users"]) - int(row["recomputed_new_users"])
        active_users_delta = int(row["serving_active_users"]) - int(row["recomputed_active_users"])
        churn_delta = int(row["serving_churn_signal_count"]) - int(row["recomputed_churn_signal_count"])
        status = "passed"
        if any(abs(d) > count_tolerance for d in (new_users_delta, active_users_delta, churn_delta)):
            status = "failed"
        results.append(
            {
                "tenant_id": row["tenant_id"],
                "metric_date": str(row["metric_date"]),
                "status": status,
                "new_users_delta": new_users_delta,
                "active_users_delta": active_users_delta,
                "churn_signal_count_delta": churn_delta,
            }
        )
    return results


async def persist_reconciliation(
    postgres: Postgres,
    request: ReconciliationRequest,
    results: list[dict[str, Any]],
) -> None:
    for result in results:
        await postgres.execute(
            """
            insert into reconciliation_audit (
                tenant_id, metric_date, check_name, status, revenue_delta,
                order_count_delta, units_sold_delta, details, checked_by, checked_at
            )
            values ($1,$2::date,'tenant_metrics_daily_reconciliation',$3,$4,$5,$6,$7::jsonb,$8,now())
            """,
            result["tenant_id"],
            # evaluate_reconciliation() deliberately normalizes metric_date
            # to a string (so it works the same whether rows came from a
            # live asyncpg fetch with date objects, or from deterministic
            # dict fixtures in tests/reliability scenarios — plain
            # strings). asyncpg's client-side codec for a $n::date
            # parameter needs a real date object to encode, not a string,
            # so it's converted back here at the persistence boundary
            # rather than changing evaluate_reconciliation's output
            # contract that tests and reliability/scenarios/reconciliation_mismatch.py
            # already depend on.
            date.fromisoformat(result["metric_date"]),
            result["status"],
            result["revenue_delta"],
            result["order_count_delta"],
            result["units_sold_delta"],
            json.dumps(result),
            request.requested_by,
        )


async def run_reconciliation(database_url: str, request: ReconciliationRequest) -> dict[str, Any]:
    validate_request(request)
    params = [request.tenant_id, request.start_date.isoformat(), request.end_date.isoformat()]
    if request.dry_run:
        return {
            "status": "dry_run",
            "tenant_id": request.tenant_id,
            "start_date": request.start_date.isoformat(),
            "end_date": request.end_date.isoformat(),
            "sql": build_daily_metrics_reconciliation_sql().strip(),
            "params": params,
        }

    run_id = str(uuid4())
    postgres = Postgres(database_url)
    try:
        rows = await postgres.fetch(
            build_daily_metrics_reconciliation_sql(),
            request.tenant_id,
            request.start_date,
            request.end_date,
        )
        results = evaluate_reconciliation(rows, revenue_tolerance=request.revenue_tolerance)
        await persist_reconciliation(postgres, request, results)
        failed = [result for result in results if result["status"] == "failed"]
        await emit_pipeline_lineage(
            postgres,
            job_name=RECONCILIATION_PIPELINE_NAME,
            run_id=run_id,
            tenant_id=request.tenant_id,
            inputs=RECONCILIATION_SOURCE_TABLES,
            outputs=RECONCILIATION_OUTPUT_TABLES,
            status="failed" if failed else "succeeded",
            metadata={"checked_count": len(results), "failed_count": len(failed)},
        )
    finally:
        await postgres.close()

    return {
        "status": "failed" if failed else "passed",
        "run_id": run_id,
        "tenant_id": request.tenant_id,
        "results": results,
        "failed_count": len(failed),
    }


async def persist_check_result(
    postgres: Postgres,
    *,
    check_name: str,
    tenant_id: str,
    metric_date_str: str,
    status: str,
    details: dict[str, Any],
    requested_by: str,
) -> None:
    """Generic persistence for any reconciliation check whose specifics
    don't map onto the revenue-shaped revenue_delta/order_count_delta/
    units_sold_delta columns — those stay 0 here and the check-specific
    deltas live in `details` (already the case for every check; this is
    just the shared insert, not a second table or format).
    """
    await postgres.execute(
        """
        insert into reconciliation_audit (
            tenant_id, metric_date, check_name, status, revenue_delta,
            order_count_delta, units_sold_delta, details, checked_by, checked_at
        )
        values ($1,$2::date,$3,$4,0,0,0,$5::jsonb,$6,now())
        """,
        tenant_id,
        date.fromisoformat(metric_date_str),
        check_name,
        status,
        json.dumps(details),
        requested_by,
    )


async def run_payment_reconciliation(database_url: str, request: ReconciliationRequest) -> dict[str, Any]:
    validate_request(request)
    params = [request.tenant_id, request.start_date.isoformat(), request.end_date.isoformat()]
    if request.dry_run:
        return {
            "status": "dry_run",
            "tenant_id": request.tenant_id,
            "sql": build_payment_reconciliation_sql().strip(),
            "params": params,
        }

    run_id = str(uuid4())
    postgres = Postgres(database_url)
    try:
        rows = await postgres.fetch(
            build_payment_reconciliation_sql(),
            request.tenant_id,
            request.start_date,
            request.end_date,
        )
        results = evaluate_payment_reconciliation(rows)
        for result in results:
            await persist_check_result(
                postgres,
                check_name="payment_reconciliation",
                tenant_id=result["tenant_id"],
                metric_date_str=result["metric_date"],
                status=result["status"],
                details=result,
                requested_by=request.requested_by,
            )
        failed = [result for result in results if result["status"] == "failed"]
        await emit_pipeline_lineage(
            postgres,
            job_name=PAYMENT_RECONCILIATION_PIPELINE_NAME,
            run_id=run_id,
            tenant_id=request.tenant_id,
            inputs=PAYMENT_RECONCILIATION_SOURCE_TABLES,
            outputs=PAYMENT_RECONCILIATION_OUTPUT_TABLES,
            status="failed" if failed else "succeeded",
            metadata={"checked_count": len(results), "failed_count": len(failed)},
        )
    finally:
        await postgres.close()

    return {
        "status": "failed" if failed else "passed",
        "run_id": run_id,
        "tenant_id": request.tenant_id,
        "results": results,
        "failed_count": len(failed),
    }


async def run_customer_activity_reconciliation(database_url: str, request: ReconciliationRequest) -> dict[str, Any]:
    validate_request(request)
    params = [request.tenant_id, request.start_date.isoformat(), request.end_date.isoformat()]
    if request.dry_run:
        return {
            "status": "dry_run",
            "tenant_id": request.tenant_id,
            "sql": build_customer_activity_reconciliation_sql().strip(),
            "params": params,
        }

    run_id = str(uuid4())
    postgres = Postgres(database_url)
    try:
        rows = await postgres.fetch(
            build_customer_activity_reconciliation_sql(),
            request.tenant_id,
            request.start_date,
            request.end_date,
        )
        results = evaluate_customer_activity_reconciliation(rows)
        for result in results:
            await persist_check_result(
                postgres,
                check_name="customer_activity_reconciliation",
                tenant_id=result["tenant_id"],
                metric_date_str=result["metric_date"],
                status=result["status"],
                details=result,
                requested_by=request.requested_by,
            )
        failed = [result for result in results if result["status"] == "failed"]
        await emit_pipeline_lineage(
            postgres,
            job_name=CUSTOMER_ACTIVITY_RECONCILIATION_PIPELINE_NAME,
            run_id=run_id,
            tenant_id=request.tenant_id,
            inputs=CUSTOMER_ACTIVITY_RECONCILIATION_SOURCE_TABLES,
            outputs=CUSTOMER_ACTIVITY_RECONCILIATION_OUTPUT_TABLES,
            status="failed" if failed else "succeeded",
            metadata={"checked_count": len(results), "failed_count": len(failed)},
        )
    finally:
        await postgres.close()

    return {
        "status": "failed" if failed else "passed",
        "run_id": run_id,
        "tenant_id": request.tenant_id,
        "results": results,
        "failed_count": len(failed),
    }


CHECK_RUNNERS = {
    "revenue": run_reconciliation,
    "payment": run_payment_reconciliation,
    "customer_activity": run_customer_activity_reconciliation,
}


async def run_all_checks(database_url: str, request: ReconciliationRequest) -> dict[str, Any]:
    """Run every registered reconciliation check for the same
    tenant/date-range request, returning a per-check breakdown plus an
    overall status (failed if any check failed).
    """
    outcomes: dict[str, Any] = {}
    for check_name, runner in CHECK_RUNNERS.items():
        outcomes[check_name] = await runner(database_url, request)
    overall_failed = any(o.get("status") == "failed" for o in outcomes.values())
    return {
        "status": "failed" if overall_failed else "passed",
        "tenant_id": request.tenant_id,
        "checks": outcomes,
    }


def write_sql_reference(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(build_daily_metrics_reconciliation_sql().strip() + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile serving metrics against processed facts.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", "postgresql://platform:platform@localhost:5432/data_platform"))
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--requested-by", default=os.getenv("USER", "local-operator"))
    parser.add_argument("--revenue-tolerance", type=float, default=0.01)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--write-sql-reference", default=None)
    parser.add_argument(
        "--check",
        choices=[*ALL_CHECK_NAMES, "all"],
        default="revenue",
        help="Which reconciliation check to run (default: revenue, the original check — preserves prior CLI behavior). "
        "'all' runs revenue, payment, and customer_activity together.",
    )
    return parser.parse_args()


async def main_async() -> None:
    args = parse_args()
    if args.write_sql_reference:
        write_sql_reference(Path(args.write_sql_reference))
    request = ReconciliationRequest(
        tenant_id=args.tenant_id,
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
        revenue_tolerance=args.revenue_tolerance,
        requested_by=args.requested_by,
        dry_run=args.dry_run,
    )
    if args.check == "all":
        result = await run_all_checks(args.database_url, request)
    else:
        result = await CHECK_RUNNERS[args.check](args.database_url, request)
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=True))


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
