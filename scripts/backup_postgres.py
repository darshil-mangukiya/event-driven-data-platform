"""Take a real, restorable PostgreSQL backup of the local platform database.

Before this script existed, docs/disaster-recovery-runbook.md described a
"Restore latest valid snapshot plus WAL/PITR" procedure with **no snapshot-
taking mechanism anywhere in the repo** — a documented process for
infrastructure (continuous WAL archiving, point-in-time recovery) this
local docker-compose project never actually set up. This script closes that
specific gap with what a local/single-instance setup can actually do: a
`pg_dump` custom-format logical backup, plus a JSON manifest recording per-
table row counts and a checksum, so `scripts/restore_postgres.py` (and
`scripts/backup_restore_drill.py`) can verify a restore actually reproduced
the same data rather than merely "ran without error".

Requires the `pg_dump` client binary on PATH (ships with PostgreSQL; on
macOS: `brew install libpq` or `brew install postgresql@16`).

Usage:
    python scripts/backup_postgres.py \
        --database-url postgresql://platform:platform@localhost:15432/data_platform \
        --output-dir backups
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Tables backed up and verified — the platform's actual serving/processing
# tables, not every table in the schema (e.g. excludes purely transient
# cache-adjacent tables if any exist). Kept as an explicit list, matching
# the same "state the scope, don't silently assume" convention as
# scripts/validate_tenant_rls.py's EXPECTED_RLS_TABLES.
BACKED_UP_TABLES = [
    "tenant_config",
    "tenant_users",
    "tenant_products",
    "raw_events",
    "processed_orders",
    "processed_payments",
    "processed_user_sessions",
    "tenant_metrics_hourly",
    "tenant_metrics_daily",
    "alerts",
    "service_health_metrics",
    "reconciliation_audit",
    "lineage_events",
    "event_inbox",
    "event_outbox",
    "privacy_erasure_requests",
]


def _row_counts(database_url: str) -> dict[str, int]:
    import psycopg2

    counts: dict[str, int] = {}
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            for table in BACKED_UP_TABLES:
                cur.execute(
                    "select exists (select 1 from information_schema.tables where table_name = %s)",
                    (table,),
                )
                if not cur.fetchone()[0]:
                    continue  # table doesn't exist yet on this database (e.g. pre-migration) — skip, don't fail
                cur.execute(f"select count(*) from {table}")  # noqa: S608 — table name from a fixed allowlist above, not user input
                counts[table] = cur.fetchone()[0]
    finally:
        conn.close()
    return counts


def _in_container_url(database_url: str) -> str:
    """Rewrite host:port to localhost:5432 — the address the postgres
    container's own client tools use to reach the database from inside the
    container, regardless of what host/port the caller reaches it on from
    outside (e.g. the POSTGRES_HOST_PORT=15432 workaround for a native
    PostgreSQL already bound to 5432 on the host).
    """
    parsed = urlparse(database_url)
    netloc = f"{parsed.username}:{parsed.password}@localhost:5432" if parsed.username else "localhost:5432"
    return urlunparse(parsed._replace(netloc=netloc))


def resolve_docker_postgres_container() -> str | None:
    """Find a running docker-compose postgres container, if any, so backup
    and restore can shell out to *its* pg_dump/pg_restore — sidestepping
    any client/server major-version mismatch between the host's installed
    PostgreSQL client tools and the postgres:16 image docker-compose.yml
    runs. Local verification confirmed that this
    machine's host pg_dump (14.19, via Homebrew) refused to dump a
    PostgreSQL 16 server outright (`pg_dump: error: aborting because of
    server version mismatch`) — a real, reproducible failure, not a
    hypothetical one.
    """
    from reliability.injectors.reachability import docker_cli_available, docker_container_running

    if not docker_cli_available():
        return None
    return docker_container_running("postgres")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_backup(*, database_url: str, output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    db_name = urlparse(database_url).path.lstrip("/") or "data_platform"
    dump_path = output_dir / f"{db_name}-{timestamp}.dump"
    manifest_path = output_dir / f"{db_name}-{timestamp}.manifest.json"

    container = resolve_docker_postgres_container()
    if container:
        # Run pg_dump *inside* the container, guaranteed version-matched to
        # its own server, and stream the custom-format output straight to
        # the local dump file via stdout.
        with dump_path.open("wb") as dump_file:
            completed = subprocess.run(
                ["docker", "exec", container, "pg_dump", "--format=custom", _in_container_url(database_url)],
                stdout=dump_file,
                stderr=subprocess.PIPE,
                check=False,
            )
        used = f"docker exec {container} pg_dump"
    elif shutil.which("pg_dump") is not None:
        completed = subprocess.run(
            ["pg_dump", "--format=custom", f"--file={dump_path}", database_url],
            capture_output=True,
            check=False,
        )
        used = "host pg_dump"
    else:
        raise RuntimeError(
            "No usable pg_dump found: no running docker-compose postgres container, and no "
            "pg_dump on PATH. Start the local stack (`make up`) or install PostgreSQL client "
            "tools (e.g. `brew install libpq && brew link --force libpq` on macOS)."
        )

    if completed.returncode != 0:
        stderr = completed.stderr.decode() if isinstance(completed.stderr, bytes) else (completed.stderr or "")
        dump_path.unlink(missing_ok=True)
        raise RuntimeError(f"{used} failed (exit {completed.returncode}): {stderr.strip()}")

    row_counts = _row_counts(database_url)
    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "backup_method": used,
        "database_name": db_name,
        "dump_file": dump_path.name,
        "dump_file_bytes": dump_path.stat().st_size,
        "dump_file_sha256": _sha256(dump_path),
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return {"dump_path": str(dump_path), "manifest_path": str(manifest_path), **manifest}


def main() -> None:
    parser = argparse.ArgumentParser(description="Take a pg_dump backup with a verifiable row-count manifest.")
    parser.add_argument("--database-url", default=None, help="Defaults to $DATABASE_URL")
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "backups"))
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    import os

    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        print("Provide --database-url or set $DATABASE_URL", file=sys.stderr)
        raise SystemExit(2)

    result = run_backup(database_url=database_url, output_dir=Path(args.output_dir))
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
