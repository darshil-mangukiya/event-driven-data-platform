from __future__ import annotations

import argparse
import time

import httpx

DEFAULT_TARGETS = [
    "http://localhost:8001/health",
    "http://localhost:8002/health",
    "http://localhost:8003/health",
    "http://localhost:8004/health",
    "http://localhost:8005/health",
]


def wait_for_targets(targets: list[str], timeout_seconds: int, poll_seconds: float = 2.0) -> dict[str, bool]:
    deadline = time.monotonic() + timeout_seconds
    status = {target: False for target in targets}
    with httpx.Client(timeout=5) as client:
        while time.monotonic() < deadline and not all(status.values()):
            for target in targets:
                if status[target]:
                    continue
                try:
                    response = client.get(target)
                    status[target] = response.status_code < 500
                except httpx.HTTPError:
                    status[target] = False
            if not all(status.values()):
                time.sleep(poll_seconds)
    return status


def main() -> None:
    parser = argparse.ArgumentParser(description="Wait for local demo services.")
    parser.add_argument("--target", action="append", default=[])
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args()
    targets = args.target or DEFAULT_TARGETS
    status = wait_for_targets(targets, args.timeout_seconds)
    for target, healthy in status.items():
        print(f"{target} {'ok' if healthy else 'unavailable'}")
    if not all(status.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
