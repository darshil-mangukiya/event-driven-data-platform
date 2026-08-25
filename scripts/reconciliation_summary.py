from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any

from platform_shared.database import Postgres


def build_summary_sql() -> str:
    return """
    select
        tenant_id,
        checked_at::date as checked_date,
        count(*) as checks_run,
        count(*) filter (where status = 'passed') as passed_checks,
        count(*) filter (where status = 'failed') as failed_checks,
        max(abs(revenue_delta)) as max_abs_revenue_delta,
        max(abs(order_count_delta)) as max_abs_order_count_delta,
        max(abs(units_sold_delta)) as max_abs_units_sold_delta,
        max(checked_at) as latest_checked_at
    from reconciliation_audit
    where checked_at >= now() - ($1::int * interval '1 day')
      and ($2::text is null or tenant_id = $2)
    group by 1, 2
    order by checked_date desc, tenant_id;
    """


def summarize_reconciliation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    failed_rows = [row for row in rows if int(row["failed_checks"]) > 0]
    return {
        "status": "failed" if failed_rows else "passed",
        "tenant_count": len({row["tenant_id"] for row in rows}),
        "days": len({str(row["checked_date"]) for row in rows}),
        "checks_run": sum(int(row["checks_run"]) for row in rows),
        "failed_checks": sum(int(row["failed_checks"]) for row in rows),
        "rows": rows,
    }


async def run_summary(database_url: str, *, days: int, tenant_id: str | None, dry_run: bool) -> dict[str, Any]:
    params = [days, tenant_id]
    if dry_run:
        return {"status": "dry_run", "sql": build_summary_sql().strip(), "params": params}

    postgres = Postgres(database_url)
    try:
        rows = await postgres.fetch(build_summary_sql(), days, tenant_id)
    finally:
        await postgres.close()
    return summarize_reconciliation(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize recent reconciliation audit results.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", "postgresql://platform:platform@localhost:5432/data_platform"))
    parser.add_argument("--tenant-id", default=None)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


async def main_async() -> None:
    args = parse_args()
    result = await run_summary(
        args.database_url,
        days=args.days,
        tenant_id=args.tenant_id,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, default=str, indent=2 if args.pretty else None, sort_keys=True))


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
