from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from platform_shared.database import Postgres


@dataclass(frozen=True)
class QualityCheckResult:
    check_name: str
    check_category: str
    tenant_id: str | None
    status: str
    severity: str
    observed_value: float | None
    threshold_value: float | None
    details: dict[str, Any]


def quality_score(results: list[QualityCheckResult]) -> dict[str, Any]:
    failed = sum(1 for result in results if result.status == "failed")
    warnings = sum(1 for result in results if result.status == "warning")
    critical = sum(1 for result in results if result.status == "failed" and result.severity == "critical")
    passed = sum(1 for result in results if result.status == "passed")
    total = max(len(results), 1)
    score = max(0.0, 100.0 - failed * 20.0 - warnings * 7.5 - critical * 15.0)
    return {
        "quality_score": round(score, 2),
        "passed_checks": passed,
        "failed_checks": failed,
        "warning_checks": warnings,
        "critical_checks": critical,
        "total_checks": total,
    }


def _numeric(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


async def run_checks(postgres: Postgres, tenant_id: str, freshness_minutes: int) -> list[QualityCheckResult]:
    results: list[QualityCheckResult] = []

    freshness = await postgres.fetchrow(
        """
        select extract(epoch from (now() - max(event_timestamp))) / 60 as minutes_since_last_event
        from raw_events
        where tenant_id = $1
        """,
        tenant_id,
    )
    minutes_since_last_event = _numeric(freshness["minutes_since_last_event"] if freshness else None)
    if minutes_since_last_event is None:
        freshness_status = "warning"
    elif minutes_since_last_event <= freshness_minutes:
        freshness_status = "passed"
    else:
        freshness_status = "failed"
    results.append(
        QualityCheckResult(
            check_name="raw_event_freshness",
            check_category="freshness",
            tenant_id=tenant_id,
            status=freshness_status,
            severity="critical",
            observed_value=minutes_since_last_event,
            threshold_value=float(freshness_minutes),
            details={"description": "Minutes since latest raw event."},
        )
    )

    null_keys = await postgres.fetchrow(
        """
        select count(*) as invalid_rows
        from raw_events
        where tenant_id = $1
          and (event_id is null or event_type is null or payload is null)
        """,
        tenant_id,
    )
    invalid_rows = int(null_keys["invalid_rows"] if null_keys else 0)
    results.append(
        QualityCheckResult(
            check_name="raw_event_required_fields",
            check_category="validity",
            tenant_id=tenant_id,
            status="passed" if invalid_rows == 0 else "failed",
            severity="critical",
            observed_value=float(invalid_rows),
            threshold_value=0,
            details={"description": "Raw event rows missing required contract fields."},
        )
    )

    duplicate_orders = await postgres.fetchrow(
        """
        select coalesce(sum(row_count - 1), 0) as duplicate_count
        from (
            select event_id, count(*) as row_count
            from processed_orders
            where tenant_id = $1
            group by event_id
            having count(*) > 1
        ) duplicates
        """,
        tenant_id,
    )
    duplicate_count = int(duplicate_orders["duplicate_count"] if duplicate_orders else 0)
    results.append(
        QualityCheckResult(
            check_name="processed_order_event_uniqueness",
            check_category="uniqueness",
            tenant_id=tenant_id,
            status="passed" if duplicate_count == 0 else "failed",
            severity="high",
            observed_value=float(duplicate_count),
            threshold_value=0,
            details={"description": "Duplicate processed order event IDs."},
        )
    )

    negative_order_revenue = await postgres.fetchrow(
        """
        select count(*) as invalid_rows
        from processed_orders
        where tenant_id = $1
          and (gross_revenue < 0 or net_revenue < 0 or unit_price < 0 or quantity < 1)
        """,
        tenant_id,
    )
    negative_revenue_rows = int(negative_order_revenue["invalid_rows"] if negative_order_revenue else 0)
    results.append(
        QualityCheckResult(
            check_name="processed_order_revenue_ranges",
            check_category="validity",
            tenant_id=tenant_id,
            status="passed" if negative_revenue_rows == 0 else "failed",
            severity="critical",
            observed_value=float(negative_revenue_rows),
            threshold_value=0,
            details={"description": "Orders must not contain negative revenue, negative unit price, or zero quantity."},
        )
    )

    invalid_payment_status = await postgres.fetchrow(
        """
        select count(*) as invalid_rows
        from processed_payments
        where tenant_id = $1
          and status not in ('authorized', 'captured', 'failed')
        """,
        tenant_id,
    )
    invalid_payment_rows = int(invalid_payment_status["invalid_rows"] if invalid_payment_status else 0)
    results.append(
        QualityCheckResult(
            check_name="processed_payment_status_domain",
            check_category="validity",
            tenant_id=tenant_id,
            status="passed" if invalid_payment_rows == 0 else "failed",
            severity="high",
            observed_value=float(invalid_payment_rows),
            threshold_value=0,
            details={"description": "Payment status must stay inside the v1 contract domain."},
        )
    )

    orphan_products = await postgres.fetchrow(
        """
        select count(*) as orphan_rows
        from processed_orders o
        left join tenant_products p
          on p.tenant_id = o.tenant_id
         and p.product_id = o.product_id
        where o.tenant_id = $1
          and p.product_id is null
        """,
        tenant_id,
    )
    orphan_product_rows = int(orphan_products["orphan_rows"] if orphan_products else 0)
    results.append(
        QualityCheckResult(
            check_name="processed_order_product_reference",
            check_category="referential_integrity",
            tenant_id=tenant_id,
            status="passed" if orphan_product_rows == 0 else "warning",
            severity="medium",
            observed_value=float(orphan_product_rows),
            threshold_value=0,
            details={"description": "Processed orders should resolve to tenant product metadata."},
        )
    )

    metric_ranges = await postgres.fetchrow(
        """
        select count(*) as invalid_metric_rows
        from tenant_metrics_daily
        where tenant_id = $1
          and (
              gross_revenue < 0 or net_revenue < 0 or order_count < 0
              or payment_success_count < 0 or payment_failure_count < 0
          )
        """,
        tenant_id,
    )
    invalid_metric_rows = int(metric_ranges["invalid_metric_rows"] if metric_ranges else 0)
    results.append(
        QualityCheckResult(
            check_name="tenant_metric_non_negative_ranges",
            check_category="validity",
            tenant_id=tenant_id,
            status="passed" if invalid_metric_rows == 0 else "failed",
            severity="critical",
            observed_value=float(invalid_metric_rows),
            threshold_value=0,
            details={"description": "Daily metrics should never be negative."},
        )
    )

    row_counts = await postgres.fetchrow(
        """
        with daily_counts as (
            select event_timestamp::date as event_date, count(*) as event_count
            from raw_events
            where tenant_id = $1
              and event_timestamp >= current_date - interval '8 day'
            group by 1
        ),
        current_day as (
            select coalesce(max(event_count) filter (where event_date = current_date), 0) as today_count
            from daily_counts
        ),
        baseline as (
            select avg(event_count) as avg_previous_count
            from daily_counts
            where event_date < current_date
        )
        select today_count, avg_previous_count
        from current_day, baseline
        """,
        tenant_id,
    )
    today_count = _numeric(row_counts["today_count"] if row_counts else 0) or 0
    avg_previous_count = _numeric(row_counts["avg_previous_count"] if row_counts else None)
    if avg_previous_count is None or avg_previous_count == 0:
        anomaly_status = "warning"
    else:
        ratio = today_count / avg_previous_count
        anomaly_status = "passed" if 0.4 <= ratio <= 2.5 else "warning"
    results.append(
        QualityCheckResult(
            check_name="raw_event_volume_anomaly",
            check_category="anomaly",
            tenant_id=tenant_id,
            status=anomaly_status,
            severity="medium",
            observed_value=today_count,
            threshold_value=avg_previous_count,
            details={"description": "Today event volume compared with trailing daily average."},
        )
    )

    return results


async def persist_results(postgres: Postgres, tenant_id: str, results: list[QualityCheckResult]) -> None:
    for result in results:
        await postgres.execute(
            """
            insert into data_quality_check_results (
                check_name, check_category, tenant_id, status, severity,
                observed_value, threshold_value, details, checked_at
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,now())
            """,
            result.check_name,
            result.check_category,
            result.tenant_id,
            result.status,
            result.severity,
            result.observed_value,
            result.threshold_value,
            json.dumps(result.details),
        )

    score = quality_score(results)
    await postgres.execute(
        """
        insert into data_quality_score_daily (
            tenant_id, score_date, quality_score, passed_checks, failed_checks,
            warning_checks, critical_checks, updated_at
        )
        values ($1,$2,$3,$4,$5,$6,$7,now())
        on conflict (tenant_id, score_date) do update set
            quality_score = excluded.quality_score,
            passed_checks = excluded.passed_checks,
            failed_checks = excluded.failed_checks,
            warning_checks = excluded.warning_checks,
            critical_checks = excluded.critical_checks,
            updated_at = now()
        """,
        tenant_id,
        date.today(),
        score["quality_score"],
        score["passed_checks"],
        score["failed_checks"],
        score["warning_checks"],
        score["critical_checks"],
    )


async def run(args: argparse.Namespace) -> None:
    postgres = Postgres(args.database_url)
    tenants = args.tenant_id
    if not tenants:
        rows = await postgres.fetch("select tenant_id from tenant_config where is_active = true order by tenant_id")
        tenants = [row["tenant_id"] for row in rows]

    payload: dict[str, Any] = {"tenants": []}
    for tenant_id in tenants:
        results = await run_checks(postgres, tenant_id, args.freshness_minutes)
        score = quality_score(results)
        if not args.dry_run:
            await persist_results(postgres, tenant_id, results)
        payload["tenants"].append(
            {
                "tenant_id": tenant_id,
                "score": score,
                "results": [asdict(result) for result in results],
            }
        )
    await postgres.close()
    print(json.dumps(payload, default=str, indent=2 if args.pretty else None, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run tenant-aware data quality checks.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", "postgresql://platform:platform@localhost:5432/data_platform"))
    parser.add_argument("--tenant-id", action="append", default=[])
    parser.add_argument("--freshness-minutes", type=int, default=180)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
