from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_results(results_dir: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for path in sorted(results_dir.glob("*.json")):
        results.append(json.loads(path.read_text()))
    return results


def render_markdown(results: list[dict[str, Any]]) -> str:
    lines = [
        "# Benchmark Evidence",
        "",
        "This report is generated from local benchmark result JSON files. It separates measured local runs from production-scale architecture targets.",
        "",
    ]
    if not results:
        lines.extend(
            [
                "No benchmark JSON files were found.",
                "",
                "Generate one with:",
                "",
                "```bash",
                "python scripts/load_test_events.py --output benchmarks/results/local-run.json",
                "python scripts/benchmark_report.py --output docs/benchmark-evidence.md",
                "```",
            ]
        )
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            "| Benchmark | Tenant | Events | Events/sec | Failures | p50 ms | p95 ms | p99 ms | Max ms |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for result in results:
        lines.append(
            "| {benchmark_name} | {tenant_id} | {events} | {events_per_second} | {failure_batches} | {p50_latency_ms} | {p95_latency_ms} | {p99_latency_ms} | {max_latency_ms} |".format(
                **result
            )
        )

    best = max(results, key=lambda result: result.get("events_per_second", 0))
    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- Best local throughput observed: `{best['events_per_second']}` events/sec in `{best['benchmark_name']}`.",
            "- Production-scale statements in this project are design targets and require distributed load generation, managed Kafka sizing, and database write-path profiling.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Render benchmark JSON files into a Markdown evidence report.")
    parser.add_argument("--results-dir", default="benchmarks/results")
    parser.add_argument("--output", default="docs/benchmark-evidence.md")
    args = parser.parse_args()

    results = load_results(Path(args.results_dir))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_markdown(results))
    print(f"wrote {output}")


if __name__ == "__main__":
    main()

