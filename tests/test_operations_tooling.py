from __future__ import annotations

import json
import random

from scripts.generate_synthetic_events_v2 import generate_events
from scripts.load_test_events import benchmark_result, order_event, percentile
from scripts.run_data_quality_checks import QualityCheckResult, quality_score


def test_quality_score_penalizes_warnings_failures_and_critical_failures() -> None:
    results = [
        QualityCheckResult("freshness", "freshness", "tenant_demo", "passed", "critical", 1, 180, {}),
        QualityCheckResult("volume", "anomaly", "tenant_demo", "warning", "medium", 20, 100, {}),
        QualityCheckResult("ranges", "validity", "tenant_demo", "failed", "critical", 1, 0, {}),
    ]

    score = quality_score(results)

    assert score["passed_checks"] == 1
    assert score["warning_checks"] == 1
    assert score["failed_checks"] == 1
    assert score["critical_checks"] == 1
    assert score["quality_score"] == 57.5


def test_benchmark_result_computes_percentiles_and_throughput() -> None:
    assert percentile([10, 20, 30, 40, 50], 95) == 50

    result = benchmark_result(
        benchmark_name="unit",
        tenant_id="tenant_demo",
        target_url="http://localhost:8001",
        total_events=100,
        elapsed_seconds=2,
        failure_batches=1,
        latencies=[10, 20, 30, 40, 50],
        batches=5,
        batch_size=20,
    )

    assert result["events_per_second"] == 50
    assert result["failure_rate"] == 0.2
    assert result["p50_latency_ms"] == 30
    assert result["p95_latency_ms"] == 50


def test_load_event_ids_are_reproducible_from_seed() -> None:
    random.seed(62020)
    first = order_event("tenant_demo")
    random.seed(62020)
    second = order_event("tenant_demo")

    assert first == second
    assert first["tenant_id"] == "tenant_demo"
    assert first["event_id"] == first["idempotency_key"]


def test_synthetic_generator_v2_is_deterministic_and_contract_valid() -> None:
    events_a = generate_events(5, seed=7)
    events_b = generate_events(5, seed=7)

    assert events_a == events_b
    for raw in events_a:
        parsed = json.loads(raw)
        assert parsed["tenant_id"].startswith("tenant_")
        assert parsed["source_service"] == "synthetic-generator-v2"
        assert parsed["payload"]
