"""Local developer-environment diagnostic.

This is deliberately a different concern from `scripts/platform_preflight.py`:
preflight answers "is the platform's data/contracts/governance state healthy
enough to release", running entirely inside the repo's own Python process
against files already checked out. This script answers "is *your machine*
set up to run this repo at all" — Python version, whether Docker is
installed and its daemon actually reachable, whether `.env` exists, and
whether the host ports docker-compose.yml wants to bind are already taken by
something else.

That last check exists because of a real, repeatedly-hit problem in this
project's own local testing: a native (non-Docker) PostgreSQL on this
machine already binds 127.0.0.1:5432, so every `docker compose up postgres`
against the default port silently either fails to bind or (worse) succeeds
against the wrong database, and this has cost real debugging time across
multiple platform components. `docker-compose.yml` already has the
escape hatch (`POSTGRES_HOST_PORT`); this script's job is to notice the
conflict *before* you run into it and tell you the one-line fix, rather than
letting you rediscover it the hard way.

Usage:
    python scripts/dev_doctor.py [--pretty] [--output-json PATH]

Exit code is 0 if there are no FAIL results (WARN results, e.g. Docker not
installed at all, don't fail the check — this repo's test suite already
degrades gracefully without Docker), 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import shutil
import socket
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MIN_PYTHON = (3, 11)

# host_port -> (compose_service, description, env_override) — the ports
# docker-compose.yml binds on the host by default. env_override is the
# docker-compose.yml environment variable that changes the host-side port,
# or None if compose does not expose one for that service.
COMPOSE_HOST_PORTS: list[tuple[int, str, str, str | None]] = [
    (5432, "postgres", "PostgreSQL", "POSTGRES_HOST_PORT"),
    (6379, "redis", "Redis", None),
    (9092, "kafka", "Kafka (internal listener)", None),
    (29092, "kafka", "Kafka (host listener)", None),
    (9090, "prometheus", "Prometheus", None),
    (3000, "grafana", "Grafana", None),
    (9001, "minio", "MinIO console", None),
    (8001, "ingestion-service", "Ingestion API", None),
    (8003, "analytics-service", "Analytics API", None),
    (8004, "metadata-service", "Metadata API", None),
    (8005, "demo-dashboard", "Demo dashboard", None),
    (8006, "ops-console", "Ops console", None),
]


@dataclass(frozen=True)
class DoctorResult:
    name: str
    status: str  # "pass" | "warn" | "fail"
    detail: str
    fix: str | None = None


def _port_in_use(port: int, host: str = "127.0.0.1", timeout: float = 0.3) -> bool:
    """True if something is already listening on host:port on this machine."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0


def check_python_version() -> DoctorResult:
    current = sys.version_info[:2]
    if current >= MIN_PYTHON:
        return DoctorResult(
            "python_version",
            "pass",
            f"Python {current[0]}.{current[1]} (>= {MIN_PYTHON[0]}.{MIN_PYTHON[1]} required)",
        )
    return DoctorResult(
        "python_version",
        "fail",
        f"Python {current[0]}.{current[1]} found, but pyproject.toml requires >= {MIN_PYTHON[0]}.{MIN_PYTHON[1]}",
        fix="Install a newer Python and recreate your virtual environment.",
    )


def check_venv_active() -> DoctorResult:
    in_venv = sys.prefix != getattr(sys, "base_prefix", sys.prefix)
    if in_venv:
        return DoctorResult("virtualenv", "pass", f"Running inside a virtual environment ({sys.prefix})")
    return DoctorResult(
        "virtualenv",
        "warn",
        "Not running inside a virtual environment — dependencies will install into the system/base Python",
        fix="python -m venv .venv-upgrade && source .venv-upgrade/bin/activate && make setup",
    )


def check_env_file() -> DoctorResult:
    env_path = PROJECT_ROOT / ".env"
    example_path = PROJECT_ROOT / ".env.example"
    if env_path.exists():
        return DoctorResult("env_file", "pass", ".env exists")
    if example_path.exists():
        return DoctorResult(
            "env_file",
            "warn",
            ".env does not exist yet (docker compose reads it for local secrets/config)",
            fix="cp .env.example .env",
        )
    return DoctorResult(
        "env_file",
        "fail",
        "Neither .env nor .env.example exists",
        fix="Restore .env.example from version control.",
    )


def check_docker() -> list[DoctorResult]:
    results: list[DoctorResult] = []
    if shutil.which("docker") is None:
        results.append(
            DoctorResult(
                "docker_cli",
                "warn",
                "docker CLI not found on PATH — Docker-dependent make targets (up, streaming-demo, ...) won't work, but the pytest suite still runs and skips/degrades any check that needs live infra",
                fix="Install Docker Desktop (or an equivalent Docker Engine) if you want to run the full local stack.",
            )
        )
        return results
    results.append(DoctorResult("docker_cli", "pass", "docker CLI found"))

    try:
        completed = subprocess.run(
            ["docker", "info"], capture_output=True, text=True, timeout=5, check=False
        )
        if completed.returncode == 0:
            results.append(DoctorResult("docker_daemon", "pass", "Docker daemon is reachable"))
        else:
            results.append(
                DoctorResult(
                    "docker_daemon",
                    "warn",
                    "docker CLI is installed but the daemon isn't responding",
                    fix="Start Docker Desktop, then re-run `make doctor`.",
                )
            )
    except (subprocess.TimeoutExpired, OSError):
        results.append(
            DoctorResult(
                "docker_daemon",
                "warn",
                "Could not reach the Docker daemon (timed out or not running)",
                fix="Start Docker Desktop, then re-run `make doctor`.",
            )
        )
    return results


def check_compose_ports() -> list[DoctorResult]:
    """Flag host ports docker-compose.yml wants that are already bound by
    something else on this machine — the recurring "native Postgres already
    owns 5432" class of problem this repo has hit multiple times.
    """
    results: list[DoctorResult] = []
    seen_ports: set[int] = set()
    for port, service, label, env_override in COMPOSE_HOST_PORTS:
        if port in seen_ports:
            continue
        seen_ports.add(port)
        if _port_in_use(port):
            if env_override:
                fix = (
                    f"Something is already listening on port {port} (likely a native, non-Docker "
                    f"install). Use the existing override instead of fighting it: "
                    f"`{env_override}=15432 docker compose up -d {service}` (adjust the port and "
                    f"DATABASE_URL/.env accordingly)."
                )
            else:
                fix = (
                    f"Something is already listening on port {port}. Stop that process, or edit "
                    f"docker-compose.yml's port mapping for `{service}` if you need both running."
                )
            results.append(
                DoctorResult(
                    f"port_{port}",
                    "warn",
                    f"Port {port} ({label}, service `{service}`) is already in use on this machine",
                    fix=fix,
                )
            )
        else:
            results.append(DoctorResult(f"port_{port}", "pass", f"Port {port} ({label}) is free"))
    return results


def check_pg_client_tools() -> DoctorResult:
    """Informational only: scripts/backup_postgres.py and restore_postgres.py
    prefer running pg_dump/pg_restore inside a running docker-compose
    postgres container (version-matched to its own server) and only fall
    back to the host's own binaries, so a missing/mismatched host pg_dump
    does not block `make backup-restore-drill` as long as `make up` (or at
    least the postgres service) is running. It only matters on its own for
    a non-Docker, native PostgreSQL setup.
    """
    if shutil.which("pg_dump") and shutil.which("pg_restore"):
        return DoctorResult("pg_client_tools", "pass", "pg_dump/pg_restore found on PATH")
    return DoctorResult(
        "pg_client_tools",
        "warn",
        "pg_dump/pg_restore not found on PATH — fine if you only use Docker Postgres "
        "(scripts/backup_postgres.py runs pg_dump inside the container instead), but "
        "required for backup/restore against a native, non-Docker PostgreSQL",
        fix="Install PostgreSQL client tools (e.g. `brew install libpq && brew link --force libpq` on macOS).",
    )


def check_requirements_installed() -> DoctorResult:
    try:
        import fastapi  # noqa: F401
        import psycopg2  # noqa: F401
        import pytest  # noqa: F401

        import redis  # noqa: F401
    except ImportError as exc:
        return DoctorResult(
            "python_dependencies",
            "fail",
            f"A core dependency failed to import: {exc}",
            fix="pip install -r requirements.txt",
        )
    return DoctorResult("python_dependencies", "pass", "Core Python dependencies import cleanly")


@dataclass
class DoctorReport:
    generated_at: str
    status: str
    results: list[DoctorResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "generated_at": self.generated_at,
            "status": self.status,
            "checks": [
                {"name": r.name, "status": r.status, "detail": r.detail, "fix": r.fix}
                for r in self.results
            ],
        }


def run_doctor() -> DoctorReport:
    results: list[DoctorResult] = [
        check_python_version(),
        check_venv_active(),
        check_env_file(),
        check_requirements_installed(),
        check_pg_client_tools(),
        *check_docker(),
        *check_compose_ports(),
    ]
    status = "fail" if any(r.status == "fail" for r in results) else "pass"
    return DoctorReport(generated_at=datetime.now(timezone.utc).isoformat(), status=status, results=results)


_ICONS = {"pass": "✅", "warn": "⚠️ ", "fail": "❌"}


def render_text(report: DoctorReport) -> str:
    lines = [f"Developer environment check — status: {report.status}", ""]
    for result in report.results:
        icon = _ICONS.get(result.status, "?")
        lines.append(f"{icon} {result.name}: {result.detail}")
        if result.fix:
            lines.append(f"   fix: {result.fix}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose the local developer environment for this repo.")
    parser.add_argument("--pretty", action="store_true", help="Print JSON instead of the human-readable summary.")
    parser.add_argument("--output-json", default=None, help="Optionally write the JSON report to this path.")
    args = parser.parse_args()

    report = run_doctor()

    if args.output_json:
        Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_json).write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n")

    if args.pretty:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        print(render_text(report))

    if report.status == "fail":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
