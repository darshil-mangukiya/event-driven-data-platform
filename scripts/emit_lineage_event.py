from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from platform_shared.database import Postgres


def build_lineage_event(
    *,
    job_name: str,
    run_id: str | None,
    tenant_id: str | None,
    inputs: list[str],
    outputs: list[str],
    status: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "eventType": "COMPLETE" if status == "succeeded" else "FAIL",
        # A real datetime, not .isoformat() — insert_lineage_event() binds
        # this straight to a `timestamptz` column via asyncpg, which (unlike
        # psycopg2) does not implicitly cast a string to a timestamp and
        # raises InvalidArgumentError instead. json.dumps(..., default=str)
        # handles serialization wherever this event is written to JSON.
        "eventTime": datetime.now(timezone.utc),
        "job": {"namespace": "data-platform-system", "name": job_name},
        "run": {"runId": run_id or str(uuid4())},
        "inputs": [{"namespace": "postgres", "name": name} for name in inputs],
        "outputs": [{"namespace": "postgres", "name": name} for name in outputs],
        "facets": {
            "tenant": {"tenant_id": tenant_id},
            "status": {"status": status},
            "metadata": metadata or {},
        },
    }


async def insert_lineage_event(postgres: Postgres, event: dict[str, Any]) -> None:
    """The actual insert, against an already-connected Postgres instance.

    Split out from persist_lineage_event() so callers that already own a
    connection (a pipeline mid-run, not a one-shot CLI invocation — see
    lineage/events.py) can reuse this without opening a second connection
    underneath the one they're already holding.
    """
    await postgres.execute(
        """
        insert into lineage_events (
            event_type, job_name, run_id, tenant_id, input_datasets,
            output_datasets, status, event_timestamp, metadata
        )
        values ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9::jsonb)
        """,
        event["eventType"],
        event["job"]["name"],
        event["run"]["runId"],
        event["facets"]["tenant"]["tenant_id"],
        json.dumps(event["inputs"]),
        json.dumps(event["outputs"]),
        event["facets"]["status"]["status"],
        event["eventTime"],
        json.dumps(event["facets"]["metadata"]),
    )


async def persist_lineage_event(database_url: str, event: dict[str, Any]) -> None:
    """CLI entrypoint: opens its own connection, inserts, closes. For
    pipeline code that already holds a connection, call
    insert_lineage_event(postgres, event) directly instead — see
    lineage/events.py.
    """
    postgres = Postgres(database_url)
    try:
        await insert_lineage_event(postgres, event)
    finally:
        await postgres.close()


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


async def main_async() -> None:
    parser = argparse.ArgumentParser(description="Emit an OpenLineage-style event locally or to Postgres.")
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--tenant-id", default=None)
    parser.add_argument("--inputs", default="")
    parser.add_argument("--outputs", default="")
    parser.add_argument("--status", choices=["succeeded", "failed"], default="succeeded")
    parser.add_argument("--metadata-json", default="{}")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", "postgresql://platform:platform@localhost:5432/data_platform"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    event = build_lineage_event(
        job_name=args.job_name,
        run_id=args.run_id,
        tenant_id=args.tenant_id,
        inputs=parse_csv(args.inputs),
        outputs=parse_csv(args.outputs),
        status=args.status,
        metadata=json.loads(args.metadata_json),
    )
    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(event, indent=2, sort_keys=True, default=str) + "\n")
    if not args.dry_run:
        await persist_lineage_event(args.database_url, event)
    print(json.dumps({"status": "dry_run" if args.dry_run else "persisted", "event": event}, indent=2, sort_keys=True, default=str))


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
