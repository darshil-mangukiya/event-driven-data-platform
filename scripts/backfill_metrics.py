from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import uuid4

from platform_shared.database import Postgres

from lineage.events import emit_pipeline_lineage

BACKFILL_PIPELINE_NAME = "backfill_tenant_metrics_daily"
BACKFILL_SOURCE_TABLES = ["processed_orders", "processed_payments", "processed_user_sessions"]
BACKFILL_OUTPUT_TABLES = ["tenant_metrics_daily"]


@dataclass(frozen=True)
class BackfillRequest:
    tenant_id: str
    start_date: date
    end_date: date
    requested_by: str
    pipeline_name: str = BACKFILL_PIPELINE_NAME
    dry_run: bool = False
    max_window_days: int = 31
    allow_large_window: bool = False


def build_daily_metrics_delete_sql() -> str:
    return """
    delete from tenant_metrics_daily
    where tenant_id = $1
      and metric_date between $2 and $3;
    """


def build_daily_metrics_insert_sql() -> str:
    return """
    insert into tenant_metrics_daily (
        tenant_id, metric_date, gross_revenue, net_revenue, order_count, units_sold,
        new_users, active_users, churn_signal_count, payment_success_count,
        payment_failure_count, marketing_spend, marketing_attributed_revenue,
        events_processed, updated_at
    )
    with dates as (
        select generate_series($2::date, $3::date, interval '1 day')::date as metric_date
    ),
    orders as (
        select
            tenant_id,
            event_timestamp::date as metric_date,
            sum(gross_revenue) as gross_revenue,
            sum(net_revenue) as net_revenue,
            count(*) as order_count,
            sum(quantity) as units_sold,
            sum(case when marketing_campaign_id is not null then 3.50 else 0 end) as marketing_spend,
            sum(case when marketing_campaign_id is not null then net_revenue else 0 end) as marketing_attributed_revenue,
            count(*) as order_events
        from processed_orders
        where tenant_id = $1
          and event_timestamp::date between $2 and $3
        group by 1, 2
    ),
    payments as (
        select
            tenant_id,
            event_timestamp::date as metric_date,
            count(*) filter (where status in ('authorized', 'captured')) as payment_success_count,
            count(*) filter (where status = 'failed') as payment_failure_count,
            count(*) as payment_events
        from processed_payments
        where tenant_id = $1
          and event_timestamp::date between $2 and $3
        group by 1, 2
    ),
    users as (
        select
            tenant_id,
            event_timestamp::date as metric_date,
            count(distinct user_id) filter (where action = 'signed_up') as new_users,
            count(distinct user_id) as active_users,
            count(*) filter (where action in ('churn_signal', 'cancel_intent')) as churn_signal_count,
            count(*) as user_events
        from processed_user_sessions
        where tenant_id = $1
          and event_timestamp::date between $2 and $3
        group by 1, 2
    )
    select
        $1 as tenant_id,
        d.metric_date,
        coalesce(o.gross_revenue, 0),
        coalesce(o.net_revenue, 0),
        coalesce(o.order_count, 0),
        coalesce(o.units_sold, 0),
        coalesce(u.new_users, 0),
        coalesce(u.active_users, 0),
        coalesce(u.churn_signal_count, 0),
        coalesce(p.payment_success_count, 0),
        coalesce(p.payment_failure_count, 0),
        coalesce(o.marketing_spend, 0),
        coalesce(o.marketing_attributed_revenue, 0),
        coalesce(o.order_events, 0) + coalesce(p.payment_events, 0) + coalesce(u.user_events, 0),
        now()
    from dates d
    left join orders o on o.metric_date = d.metric_date and o.tenant_id = $1
    left join payments p on p.metric_date = d.metric_date and p.tenant_id = $1
    left join users u on u.metric_date = d.metric_date and u.tenant_id = $1;
    """


def build_daily_metrics_backfill_sql() -> str:
    return f"{build_daily_metrics_delete_sql()}\n{build_daily_metrics_insert_sql()}"


def validate_request(request: BackfillRequest) -> None:
    if request.start_date > request.end_date:
        raise ValueError("start-date must be before or equal to end-date")
    window_days = (request.end_date - request.start_date).days + 1
    if window_days > request.max_window_days and not request.allow_large_window:
        raise ValueError(
            "date window is too large for a routine backfill; pass --allow-large-window "
            "after confirming database capacity"
        )


def build_backfill_plan(request: BackfillRequest) -> dict[str, Any]:
    params = [
        request.tenant_id,
        request.start_date.isoformat(),
        request.end_date.isoformat(),
    ]
    return {
        "pipeline_name": request.pipeline_name,
        "tenant_id": request.tenant_id,
        "start_date": request.start_date.isoformat(),
        "end_date": request.end_date.isoformat(),
        "requested_by": request.requested_by,
        "idempotency": "delete_and_recompute_tenant_date_range",
        "statements": [
            {
                "name": "delete_existing_daily_metrics",
                "sql": build_daily_metrics_delete_sql().strip(),
                "params": params,
            },
            {
                "name": "insert_recomputed_daily_metrics",
                "sql": build_daily_metrics_insert_sql().strip(),
                "params": params,
            },
        ],
        "cache_follow_up": "tenant metric API responses expire by TTL; force cache flush for urgent corrections",
    }


async def run_backfill(postgres: Postgres, request: BackfillRequest) -> dict[str, Any]:
    validate_request(request)
    plan = build_backfill_plan(request)
    if request.dry_run:
        return {
            "status": "dry_run",
            **plan,
        }

    # One run_id, generated here and reused for both the pipeline_run_log
    # row (updated in place at completion, not a second insert) and the
    # lineage event below — closing the "correlate pipeline_run_log,
    # lineage_events... by run ID" gap docs/openlineage-tracking.md
    # described as a production-evolution item nothing implemented yet.
    run_id = str(uuid4())
    await postgres.execute(
        """
        insert into pipeline_run_log (
            pipeline_run_id, pipeline_name, status, records_processed, error_message, started_at
        )
        values ($1, $2, 'running', 0, $3, now())
        """,
        run_id,
        request.pipeline_name,
        f"requested_by={request.requested_by}; tenant_id={request.tenant_id}",
    )
    try:
        statuses = await postgres.execute_transaction(
            [
                (
                    build_daily_metrics_delete_sql(),
                    [request.tenant_id, request.start_date, request.end_date],
                ),
                (
                    build_daily_metrics_insert_sql(),
                    [request.tenant_id, request.start_date, request.end_date],
                ),
            ]
        )
        await postgres.execute(
            """
            update pipeline_run_log
            set status = 'succeeded', error_message = $2, finished_at = now()
            where pipeline_run_id = $1
            """,
            run_id,
            f"tenant_id={request.tenant_id}; statuses={statuses}",
        )
        await emit_pipeline_lineage(
            postgres,
            job_name=request.pipeline_name,
            run_id=run_id,
            tenant_id=request.tenant_id,
            inputs=BACKFILL_SOURCE_TABLES,
            outputs=BACKFILL_OUTPUT_TABLES,
            status="succeeded",
            metadata={"start_date": request.start_date.isoformat(), "end_date": request.end_date.isoformat()},
        )
        return {
            "status": "succeeded",
            "run_id": run_id,
            **plan,
            "database_statuses": statuses,
        }
    except Exception as exc:
        await postgres.execute(
            """
            update pipeline_run_log
            set status = 'failed', error_message = $2, finished_at = now()
            where pipeline_run_id = $1
            """,
            run_id,
            str(exc),
        )
        await emit_pipeline_lineage(
            postgres,
            job_name=request.pipeline_name,
            run_id=run_id,
            tenant_id=request.tenant_id,
            inputs=BACKFILL_SOURCE_TABLES,
            outputs=BACKFILL_OUTPUT_TABLES,
            status="failed",
            metadata={"error": str(exc)},
        )
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill tenant daily serving metrics.")
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "postgresql://platform:platform@localhost:5432/data_platform"),
    )
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--requested-by", default=os.getenv("USER", "local-operator"))
    parser.add_argument("--pipeline-name", default=BACKFILL_PIPELINE_NAME)
    parser.add_argument("--max-window-days", type=int, default=31)
    parser.add_argument("--allow-large-window", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


async def main_async() -> None:
    args = parse_args()
    request = BackfillRequest(
        tenant_id=args.tenant_id,
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
        requested_by=args.requested_by,
        pipeline_name=args.pipeline_name,
        dry_run=args.dry_run,
        max_window_days=args.max_window_days,
        allow_large_window=args.allow_large_window,
    )
    postgres = Postgres(args.database_url)
    try:
        result = await run_backfill(postgres, request)
    finally:
        await postgres.close()
    print(json.dumps(result, default=str, indent=2 if args.pretty else None, sort_keys=True))


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
