"""Tests for the benchmark-regression workflow consolidation.

Previously, `scripts/compare_benchmarks.py` was a working, tested
comparison tool (see `tests/test_reliability_governance_tooling.py::
test_benchmark_compare_flags_throughput_regression` for its own logic
coverage) with a checked-in baseline (`samples/benchmarks/
local_ingestion_sample.json`) — but it had **zero Makefile target and zero
CI wiring**, so nothing in the repo's normal workflow ever actually ran a
regression gate; a contributor had to already know the script existed and
construct the right `--current`/`--baseline` arguments by hand.

These tests protect the new `make benchmark-compare` / `make
load-test-and-compare` wiring itself (Makefile content), not
`compare_benchmarks.py`'s comparison math, which already had coverage.
"""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_makefile_has_benchmark_compare_and_load_test_and_compare_targets() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    assert "\nbenchmark-compare:" in makefile
    assert "\nload-test-and-compare:" in makefile


def test_benchmark_compare_target_uses_scripts_compare_benchmarks() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    target_block = makefile.split("\nbenchmark-compare:")[1].split("\n\n")[0]
    assert "scripts/compare_benchmarks.py" in target_block


def test_load_test_and_compare_target_chains_load_test_then_compare() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    target_block = makefile.split("\nload-test-and-compare:")[1].split("\n\n")[0]
    assert "scripts/load_test_events.py" in target_block
    assert "scripts/compare_benchmarks.py" in target_block


def test_checked_in_baseline_sample_has_all_fields_compare_benchmarks_requires() -> None:
    import json

    baseline_path = PROJECT_ROOT / "samples" / "benchmarks" / "local_ingestion_sample.json"
    payload = json.loads(baseline_path.read_text())
    required = {"benchmark_name", "events", "events_per_second", "failure_rate", "p95_latency_ms"}
    missing = required - set(payload)
    assert not missing, f"baseline sample is missing fields compare_benchmarks.py requires: {missing}"


def test_compare_benchmarks_surfaces_host_platform_without_changing_the_gate() -> None:
    """compare_benchmarks.py surfaces
    current/baseline host_platform metadata so a human can see when a
    'failed' comparison might be explained by different machines rather
    than a code regression — but this must be purely informational and
    never change the pass/fail verdict itself.
    """
    from scripts.compare_benchmarks import compare

    baseline = {
        "benchmark_name": "b",
        "events": 100,
        "events_per_second": 594.0,
        "failure_rate": 0.0,
        "p95_latency_ms": 100.0,
        "host_platform": "machine-A",
    }
    current_same_host = {**baseline, "benchmark_name": "c", "events_per_second": 594.0, "host_platform": "machine-A"}
    current_diff_host = {**baseline, "benchmark_name": "c", "events_per_second": 352.0, "host_platform": "machine-B"}

    same_host_result = compare(current_same_host, baseline, min_throughput_ratio=0.8)
    diff_host_result = compare(current_diff_host, baseline, min_throughput_ratio=0.8)

    assert same_host_result["status"] == "passed"
    assert "matching host_platform" in same_host_result["environment_note"]

    # The gate is still strict even though the environment differs — this
    # metadata informs a human reviewer, it does not weaken the threshold.
    assert diff_host_result["status"] == "failed"
    assert "differ" in diff_host_result["environment_note"]


def test_benchmarks_results_directory_is_gitignored_but_tracked() -> None:
    """The directory itself must stay in version control (via .gitkeep) so
    `make load-test-and-compare` has somewhere to write to on a fresh
    checkout, while generated *.json result files must not be committed.
    """
    gitignore = (PROJECT_ROOT / ".gitignore").read_text()
    assert "benchmarks/results/*.json" in gitignore
    assert (PROJECT_ROOT / "benchmarks" / "results" / ".gitkeep").exists()
