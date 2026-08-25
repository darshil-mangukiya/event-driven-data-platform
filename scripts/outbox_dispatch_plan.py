from __future__ import annotations

import argparse
import json
from pathlib import Path


def lease_pending_sql() -> str:
    return """
    with next_batch as (
        select outbox_id
        from event_outbox
        where status = 'pending'
          and available_at <= now()
        order by created_at
        limit $1
        for update skip locked
    )
    update event_outbox o
    set status = 'publishing',
        locked_at = now(),
        attempts = attempts + 1,
        updated_at = now()
    from next_batch b
    where o.outbox_id = b.outbox_id
    returning o.*;
    """


def mark_published_sql() -> str:
    return """
    update event_outbox
    set status = 'published',
        published_at = now(),
        error_message = null,
        updated_at = now()
    where outbox_id = $1;
    """


def mark_failed_sql() -> str:
    return """
    update event_outbox
    set status = case when attempts >= $2 then 'failed' else 'pending' end,
        available_at = now() + ($3::int * interval '1 second'),
        error_message = $4,
        updated_at = now()
    where outbox_id = $1;
    """


def dispatch_plan(batch_size: int, retry_delay_seconds: int, max_attempts: int) -> dict[str, object]:
    return {
        "status": "dry_run",
        "batch_size": batch_size,
        "retry_delay_seconds": retry_delay_seconds,
        "max_attempts": max_attempts,
        "steps": [
            {"name": "lease_pending_events", "sql": lease_pending_sql().strip(), "params": [batch_size]},
            {"name": "publish_to_kafka", "note": "Publish each leased row to the domain topic using event_type routing."},
            {"name": "mark_published", "sql": mark_published_sql().strip(), "params": ["<outbox_id>"]},
            {
                "name": "mark_failed_or_retry",
                "sql": mark_failed_sql().strip(),
                "params": ["<outbox_id>", max_attempts, retry_delay_seconds, "<error_message>"],
            },
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the event outbox dispatch lease/retry plan.")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--retry-delay-seconds", type=int, default=30)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--write-sql-dir", default=None)
    args = parser.parse_args()

    if args.write_sql_dir:
        output_dir = Path(args.write_sql_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "lease_pending_events.sql").write_text(lease_pending_sql().strip() + "\n")
        (output_dir / "mark_published.sql").write_text(mark_published_sql().strip() + "\n")
        (output_dir / "mark_failed.sql").write_text(mark_failed_sql().strip() + "\n")

    print(json.dumps(dispatch_plan(args.batch_size, args.retry_delay_seconds, args.max_attempts), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
