"""Restore a pg_dump backup produced by scripts/backup_postgres.py and
verify it against the manifest's recorded row counts.

Requires the `pg_restore` client binary on PATH (same package as
`pg_dump`).

Usage:
    # Dry run: validates the dump/manifest pair exists and is internally
    # consistent, without touching any database.
    python scripts/restore_postgres.py --dump backups/data_platform-....dump --dry-run

    # Real restore into a target database (should usually be a scratch/
    # drill database, not the live one — see scripts/backup_restore_drill.py).
    python scripts/restore_postgres.py \
        --dump backups/data_platform-....dump \
        --target-database-url postgresql://platform:platform@localhost:15432/data_platform_restore_drill
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from backup_postgres import _in_container_url, resolve_docker_postgres_container  # noqa: E402


def _manifest_for(dump_path: Path) -> dict[str, object]:
    manifest_path = dump_path.with_suffix("").with_suffix(".manifest.json")
    if not manifest_path.exists():
        raise FileNotFoundError(f"no manifest found alongside {dump_path} (expected {manifest_path})")
    return json.loads(manifest_path.read_text())


def _row_counts(database_url: str, tables: list[str]) -> dict[str, int]:
    import psycopg2

    counts: dict[str, int] = {}
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(
                    "select exists (select 1 from information_schema.tables where table_name = %s)",
                    (table,),
                )
                if not cur.fetchone()[0]:
                    counts[table] = 0
                    continue
                cur.execute(f"select count(*) from {table}")  # noqa: S608 — table names come only from the trusted manifest, never user input
                counts[table] = cur.fetchone()[0]
    finally:
        conn.close()
    return counts


def run_restore(*, dump_path: Path, target_database_url: str, verify: bool = True) -> dict[str, object]:
    manifest = _manifest_for(dump_path)
    container = resolve_docker_postgres_container()
    if container:
        # Same rationale as backup_postgres.py: run pg_restore inside the
        # container, version-matched to its own server, streaming the dump
        # file in over stdin.
        with dump_path.open("rb") as dump_file:
            completed = subprocess.run(
                [
                    "docker",
                    "exec",
                    "-i",
                    container,
                    "pg_restore",
                    "--clean",
                    "--if-exists",
                    "--no-owner",
                    "--no-privileges",
                    f"--dbname={_in_container_url(target_database_url)}",
                ],
                stdin=dump_file,
                capture_output=True,
                text=True,
                check=False,
            )
    elif shutil.which("pg_restore") is not None:
        completed = subprocess.run(
            [
                "pg_restore",
                "--clean",
                "--if-exists",
                "--no-owner",
                "--no-privileges",
                f"--dbname={target_database_url}",
                str(dump_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    else:
        raise RuntimeError(
            "No usable pg_restore found: no running docker-compose postgres container, and no "
            "pg_restore on PATH."
        )
    # pg_restore commonly exits non-zero on "role does not exist"/harmless
    # ownership warnings even on an otherwise-successful restore (this
    # backup is taken with --no-owner on write and --no-owner/--no-privileges
    # on restore specifically to minimize that noise) — so the real
    # correctness signal is the row-count verification below, not the exit
    # code alone. Still surface stderr for a human to read.
    result: dict[str, object] = {
        "dump_path": str(dump_path),
        "pg_restore_returncode": completed.returncode,
        "pg_restore_stderr": completed.stderr.strip(),
        "expected_row_counts": manifest["row_counts"],
    }

    if verify:
        actual = _row_counts(target_database_url, list(manifest["row_counts"].keys()))
        mismatches = {
            table: {"expected": expected, "actual": actual.get(table)}
            for table, expected in manifest["row_counts"].items()
            if actual.get(table) != expected
        }
        result["actual_row_counts"] = actual
        result["mismatches"] = mismatches
        result["status"] = "passed" if not mismatches else "failed"
    else:
        result["status"] = "restored_unverified"
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Restore a pg_dump backup and verify row counts against its manifest.")
    parser.add_argument("--dump", required=True)
    parser.add_argument("--target-database-url", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Only validate the dump/manifest pair; touch no database.")
    parser.add_argument("--no-verify", action="store_true", help="Skip post-restore row-count verification.")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    dump_path = Path(args.dump)
    if not dump_path.exists():
        print(f"dump file not found: {dump_path}", file=sys.stderr)
        raise SystemExit(2)

    if args.dry_run:
        manifest = _manifest_for(dump_path)
        print(
            json.dumps(
                {"status": "dry_run", "dump_path": str(dump_path), "manifest": manifest},
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
        )
        return

    if not args.target_database_url:
        print("Provide --target-database-url (or use --dry-run)", file=sys.stderr)
        raise SystemExit(2)

    result = run_restore(
        dump_path=dump_path, target_database_url=args.target_database_url, verify=not args.no_verify
    )
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=True))
    if result["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
