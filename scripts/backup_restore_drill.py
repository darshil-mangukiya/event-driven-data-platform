"""Run a full, real backup-and-restore drill: back up the live database,
restore it into a throwaway scratch database, verify every table's row
count matches, then drop the scratch database.

This is the actual tool behind docs/disaster-recovery-runbook.md's
"Postgres restore rehearsal" drill row ("Quarterly ... Restore timestamp,
smoke results, reconciliation report") — previously that row named
evidence a drill would produce without any tool in the repo able to run one.

Never touches the source database beyond a read-only pg_dump. Creates and
drops only the scratch database it names itself.

Usage:
    python scripts/backup_restore_drill.py \
        --database-url postgresql://platform:platform@localhost:15432/data_platform
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from backup_postgres import run_backup  # noqa: E402
from restore_postgres import run_restore  # noqa: E402


def _scratch_database_url(source_url: str, scratch_db_name: str) -> str:
    parsed = urlparse(source_url)
    return urlunparse(parsed._replace(path=f"/{scratch_db_name}"))


def _maintenance_url(source_url: str) -> str:
    """A connection to the `postgres` maintenance database, used only to
    CREATE DATABASE / DROP DATABASE the scratch database — you cannot run
    those statements while connected to the database you're creating or
    dropping.
    """
    parsed = urlparse(source_url)
    return urlunparse(parsed._replace(path="/postgres"))


def _create_scratch_database(source_url: str, scratch_db_name: str) -> None:
    import psycopg2

    conn = psycopg2.connect(_maintenance_url(source_url))
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'drop database if exists "{scratch_db_name}"')
            cur.execute(f'create database "{scratch_db_name}"')
    finally:
        conn.close()


def _drop_scratch_database(source_url: str, scratch_db_name: str) -> None:
    import psycopg2

    conn = psycopg2.connect(_maintenance_url(source_url))
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            # Terminate any lingering connections (pg_restore's own
            # connection should already be closed by the time we get here,
            # but be defensive) before dropping, or DROP DATABASE fails.
            cur.execute(
                "select pg_terminate_backend(pid) from pg_stat_activity "
                "where datname = %s and pid <> pg_backend_pid()",
                (scratch_db_name,),
            )
            cur.execute(f'drop database if exists "{scratch_db_name}"')
    finally:
        conn.close()


def run_drill(*, database_url: str, output_dir: Path, keep_scratch_db: bool = False) -> dict[str, object]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    scratch_db_name = f"data_platform_restore_drill_{timestamp}"
    scratch_url = _scratch_database_url(database_url, scratch_db_name)

    backup_result = run_backup(database_url=database_url, output_dir=output_dir)
    _create_scratch_database(database_url, scratch_db_name)
    try:
        restore_result = run_restore(
            dump_path=Path(backup_result["dump_path"]), target_database_url=scratch_url, verify=True
        )
    finally:
        if not keep_scratch_db:
            _drop_scratch_database(database_url, scratch_db_name)

    return {
        "drill_completed_at": datetime.now(timezone.utc).isoformat(),
        "status": restore_result["status"],
        "scratch_database": scratch_db_name,
        "scratch_database_kept": keep_scratch_db,
        "backup": backup_result,
        "restore": restore_result,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a full backup-then-restore drill against a scratch database.")
    parser.add_argument("--database-url", default=None, help="Defaults to $DATABASE_URL")
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "backups"))
    parser.add_argument("--keep-scratch-db", action="store_true", help="Don't drop the scratch database afterward (for manual inspection).")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    import os

    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        print("Provide --database-url or set $DATABASE_URL", file=sys.stderr)
        raise SystemExit(2)

    result = run_drill(database_url=database_url, output_dir=Path(args.output_dir), keep_scratch_db=args.keep_scratch_db)
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=True))
    if result["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
