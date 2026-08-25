"""Cheap, short-timeout reachability probes shared by every scenario.

Every scenario calls these first to decide whether it can run a step "for
real" (VERIFIED) against live infrastructure, or has to fall back to
exercising the platform's transformation and decision code with deterministic
local input and report
SIMULATED / NOT_RUN. The probes do not fabricate positive results.
"""

from __future__ import annotations

import shutil
import socket
import subprocess
from urllib.parse import urlparse


def tcp_reachable(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def postgres_reachable(database_url: str, timeout: float = 2.0) -> bool:
    try:
        import math

        import psycopg2

        # libpq's connect_timeout option only accepts whole seconds — a
        # float (e.g. the 2.0 default) raises
        # `OperationalError: invalid integer value "2.0" for connection
        # option "connect_timeout"`, which the broad except below silently
        # turned into a False result even when PostgreSQL was
        # reachable. Local initialization verification found this function
        # reported "not
        # reachable" against a database a direct psycopg2.connect() call
        # reached immediately. Round up so a sub-second timeout still
        # waits at least 1s rather than truncating to 0 (which libpq
        # treats as "no timeout", the opposite of intent).
        conn = psycopg2.connect(database_url, connect_timeout=max(1, math.ceil(timeout)))
        conn.close()
        return True
    except Exception:
        return False


def redis_reachable(redis_url: str, timeout: float = 1.5) -> bool:
    parsed = urlparse(redis_url)
    return tcp_reachable(parsed.hostname or "localhost", parsed.port or 6379, timeout=timeout)


def kafka_reachable(bootstrap_servers: str, timeout: float = 2.0) -> bool:
    first = bootstrap_servers.split(",")[0].strip()
    if ":" not in first:
        return False
    host, port_str = first.rsplit(":", 1)
    try:
        port = int(port_str)
    except ValueError:
        return False
    return tcp_reachable(host, port, timeout=timeout)


def docker_cli_available() -> bool:
    return shutil.which("docker") is not None


def docker_container_running(name_substring: str) -> str | None:
    """Return the first running container name containing ``name_substring``, if any."""
    if not docker_cli_available():
        return None
    try:
        completed = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return None
    for line in completed.stdout.splitlines():
        if name_substring in line:
            return line.strip()
    return None
