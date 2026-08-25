from __future__ import annotations

import argparse
import json
import socket
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx


def load_scenarios(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text())
    return payload["scenarios"]


def probe_target(target: str, timeout_seconds: float = 3.0) -> dict[str, Any]:
    if target.startswith("http://") or target.startswith("https://"):
        try:
            response = httpx.get(target, timeout=timeout_seconds)
            return {"reachable": response.status_code < 500, "status_code": response.status_code}
        except httpx.HTTPError as exc:
            return {"reachable": False, "error": str(exc)}

    parsed = urlparse(target)
    if parsed.scheme == "redis":
        host = parsed.hostname or "localhost"
        port = parsed.port or 6379
    elif ":" in target:
        host, raw_port = target.rsplit(":", 1)
        port = int(raw_port)
    else:
        host = target
        port = 80

    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return {"reachable": True, "host": host, "port": port}
    except OSError as exc:
        return {"reachable": False, "host": host, "port": port, "error": str(exc)}


def run_probe(scenarios: list[dict[str, Any]], dry_run: bool) -> list[dict[str, Any]]:
    results = []
    for scenario in scenarios:
        if dry_run:
            probe = {"reachable": None, "dry_run": True}
        else:
            probe = probe_target(scenario["target"])
        results.append({**scenario, "probe": probe})
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe local resilience/chaos scenarios.")
    parser.add_argument("--scenarios", default="chaos/scenarios.json")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    scenarios = load_scenarios(Path(args.scenarios))
    results = run_probe(scenarios, args.dry_run)
    payload = {"results": results}
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, indent=2, sort_keys=True))
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

