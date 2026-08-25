from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_benchmark(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    required = {"benchmark_name", "events", "events_per_second", "failure_rate", "p95_latency_ms"}
    missing = required - set(payload)
    if missing:
        raise ValueError(f"{path} missing benchmark fields: {sorted(missing)}")
    return payload


def compare(current: dict[str, Any], baseline: dict[str, Any], *, min_throughput_ratio: float) -> dict[str, Any]:
    current_eps = float(current["events_per_second"])
    baseline_eps = float(baseline["events_per_second"])
    ratio = round(current_eps / baseline_eps, 4) if baseline_eps else 0
    status = "passed" if ratio >= min_throughput_ratio and float(current["failure_rate"]) <= float(baseline["failure_rate"]) else "failed"

    current_host = current.get("host_platform")
    baseline_host = baseline.get("host_platform")
    # Informational only — this never changes `status` above. Its purpose
    # is to make it visible, right in the comparison output, when a
    # "failed" result might be explained by comparing two different
    # machines rather than an actual code regression. A human should
    # inspect this before treating a gate failure as a confirmed
    # regression; the gate itself stays strict either way (see
    # benchmarks/README.md).
    environment_note = (
        "current and baseline host_platform differ — a lower ratio here may reflect machine "
        "speed, not a code regression; inspect before treating this as a confirmed regression"
        if current_host and baseline_host and current_host != baseline_host
        else "no host_platform metadata to compare (older baseline or current result)"
        if not current_host or not baseline_host
        else "current and baseline ran on matching host_platform metadata"
    )

    return {
        "status": status,
        "current_benchmark": current["benchmark_name"],
        "baseline_benchmark": baseline["benchmark_name"],
        "throughput_ratio": ratio,
        "current_events_per_second": current_eps,
        "baseline_events_per_second": baseline_eps,
        "current_failure_rate": current["failure_rate"],
        "baseline_failure_rate": baseline["failure_rate"],
        "current_p95_latency_ms": current["p95_latency_ms"],
        "baseline_p95_latency_ms": baseline["p95_latency_ms"],
        "current_host_platform": current_host,
        "baseline_host_platform": baseline_host,
        "environment_note": environment_note,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare a benchmark result to a baseline JSON artifact.")
    parser.add_argument("--current", required=True)
    parser.add_argument("--baseline", default="samples/benchmarks/local_ingestion_sample.json")
    parser.add_argument("--min-throughput-ratio", type=float, default=0.8)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    result = compare(
        load_benchmark(Path(args.current)),
        load_benchmark(Path(args.baseline)),
        min_throughput_ratio=args.min_throughput_ratio,
    )
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=True))
    if result["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
