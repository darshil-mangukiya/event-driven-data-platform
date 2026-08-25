"""Tests for the backup/restore/DR-drill tooling:
scripts/backup_postgres.py, scripts/restore_postgres.py,
scripts/backup_restore_drill.py.

Previously, docs/disaster-recovery-runbook.md described a
"Restore latest valid snapshot plus WAL/PITR" procedure and a quarterly
"Postgres restore rehearsal" drill with **no snapshot-taking mechanism
anywhere in the repo** — a documented process this local docker-compose
project had no actual tool to run. These scripts close that gap with a real
pg_dump/pg_restore-based backup and a full backup->restore->verify->cleanup
drill against a scratch database.

The backup path handles a host/server `pg_dump` major-version mismatch. A host
`pg_dump` 14 client refuses to dump a
PostgreSQL 16 server (`pg_dump: error: aborting because of server version
mismatch`) — exactly the kind of host/container version skew a local
Docker-based project should expect. The fix (`resolve_docker_postgres_
container()`) runs pg_dump/pg_restore *inside* the running postgres
container instead, guaranteed version-matched to its own server.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, PROJECT_ROOT / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


backup_postgres = _load("backup_postgres")
restore_postgres = _load("restore_postgres")


def test_in_container_url_rewrites_host_and_port_to_localhost_5432() -> None:
    rewritten = backup_postgres._in_container_url(
        "postgresql://platform:platform@localhost:15432/data_platform"
    )
    assert rewritten == "postgresql://platform:platform@localhost:5432/data_platform"


def test_in_container_url_preserves_credentials_and_database_name() -> None:
    rewritten = backup_postgres._in_container_url(
        "postgresql://someuser:somepass@remote-host:9999/some_db"
    )
    assert "someuser:somepass@localhost:5432/some_db" in rewritten


def test_resolve_docker_postgres_container_returns_none_when_docker_cli_missing() -> None:
    with patch("reliability.injectors.reachability.docker_cli_available", return_value=False):
        assert backup_postgres.resolve_docker_postgres_container() is None


def test_resolve_docker_postgres_container_returns_the_found_container_name() -> None:
    with patch("reliability.injectors.reachability.docker_cli_available", return_value=True), patch(
        "reliability.injectors.reachability.docker_container_running", return_value="myproject-postgres-1"
    ):
        assert backup_postgres.resolve_docker_postgres_container() == "myproject-postgres-1"


def test_backed_up_tables_list_matches_demo_seed_tables() -> None:
    """Regression: if the seed data or schema grows a new tenant-scoped
    table that backup_postgres.py doesn't know about, a restore drill would
    silently under-report data loss risk. This doesn't catch every future
    addition, but it does pin the known set so a removal is deliberate.
    """
    for table in ("processed_orders", "processed_payments", "processed_user_sessions", "raw_events", "tenant_metrics_daily"):
        assert table in backup_postgres.BACKED_UP_TABLES


def test_manifest_for_raises_a_clear_error_when_manifest_file_is_missing(tmp_path) -> None:
    fake_dump = tmp_path / "some_db-20260101T000000Z.dump"
    fake_dump.write_bytes(b"not a real dump")
    with pytest.raises(FileNotFoundError, match="no manifest found"):
        restore_postgres._manifest_for(fake_dump)


def test_manifest_for_loads_a_real_manifest_file(tmp_path) -> None:
    import json

    dump_path = tmp_path / "some_db-20260101T000000Z.dump"
    dump_path.write_bytes(b"fake dump content")
    manifest_path = tmp_path / "some_db-20260101T000000Z.manifest.json"
    manifest_path.write_text(json.dumps({"row_counts": {"tenant_config": 3}}))

    manifest = restore_postgres._manifest_for(dump_path)
    assert manifest["row_counts"]["tenant_config"] == 3


def test_run_restore_reports_failed_status_when_row_counts_mismatch(tmp_path) -> None:
    """Even if pg_restore itself exits 0, a row-count mismatch after
    restore must be reported as a failed drill — pg_restore's exit code
    alone is not a trustworthy correctness signal (it commonly exits
    non-zero on harmless ownership warnings too, which is exactly why this
    verification step exists rather than trusting the exit code either
    way).
    """
    import json

    dump_path = tmp_path / "db-20260101T000000Z.dump"
    dump_path.write_bytes(b"fake")
    manifest_path = tmp_path / "db-20260101T000000Z.manifest.json"
    manifest_path.write_text(json.dumps({"row_counts": {"tenant_config": 3, "raw_events": 100}}))

    fake_completed = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()
    with patch.object(restore_postgres, "resolve_docker_postgres_container", return_value=None), patch(
        "shutil.which", return_value="/usr/bin/pg_restore"
    ), patch("subprocess.run", return_value=fake_completed), patch.object(
        restore_postgres, "_row_counts", return_value={"tenant_config": 3, "raw_events": 42}
    ):
        result = restore_postgres.run_restore(dump_path=dump_path, target_database_url="postgresql://x/y")

    assert result["status"] == "failed"
    assert result["mismatches"] == {"raw_events": {"expected": 100, "actual": 42}}


def test_run_restore_reports_passed_status_when_row_counts_match(tmp_path) -> None:
    import json

    dump_path = tmp_path / "db-20260101T000000Z.dump"
    dump_path.write_bytes(b"fake")
    manifest_path = tmp_path / "db-20260101T000000Z.manifest.json"
    manifest_path.write_text(json.dumps({"row_counts": {"tenant_config": 3}}))

    fake_completed = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()
    with patch.object(restore_postgres, "resolve_docker_postgres_container", return_value=None), patch(
        "shutil.which", return_value="/usr/bin/pg_restore"
    ), patch("subprocess.run", return_value=fake_completed), patch.object(
        restore_postgres, "_row_counts", return_value={"tenant_config": 3}
    ):
        result = restore_postgres.run_restore(dump_path=dump_path, target_database_url="postgresql://x/y")

    assert result["status"] == "passed"
    assert result["mismatches"] == {}


# ---------------------------------------------------------------------------
# Live integration: full backup -> restore -> verify -> drop drill
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_live_backup_restore_drill_round_trips_real_data(tmp_path) -> None:
    from reliability.injectors.reachability import postgres_reachable

    database_url = os.getenv("DATABASE_URL", "postgresql://platform:platform@localhost:15432/data_platform")
    if not postgres_reachable(database_url):
        pytest.skip(f"PostgreSQL not reachable at {database_url}")
    if backup_postgres.resolve_docker_postgres_container() is None:
        pytest.skip("no running docker-compose postgres container found (drill needs docker exec for a version-matched pg_dump/pg_restore)")

    backup_restore_drill = _load("backup_restore_drill")
    result = backup_restore_drill.run_drill(database_url=database_url, output_dir=tmp_path)

    assert result["status"] == "passed"
    assert result["restore"]["mismatches"] == {}
    assert result["backup"]["total_rows"] >= 0
