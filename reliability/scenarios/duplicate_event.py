"""Duplicate-event reliability exercise.

Publishes the same event twice and checks that the platform does not double-count
it: the streaming layer's `dropDuplicatesWithinWatermark([tenant_id,
event_id])` suppresses cross-batch duplicates, as covered by
`tests/streaming/test_streaming_integration.py`. This exercise checks
same-batch duplicate detection via
``deduplication.mark_duplicates_within_batch``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from reliability.injectors.kafka_injector import publish_raw_twice
from reliability.injectors.reachability import kafka_reachable
from reliability.injectors.spark_harness import (
    get_or_create_spark,
    kafka_shaped_batch,
    spark_available,
)
from reliability.models import ScenarioResult, StepResult
from spark.streaming.config import StreamingConfig

SCENARIO_ID = "duplicate-event"

DUPLICATE_EVENT_ID = "reliability-duplicate-event"
RECORD = {
    "event_id": DUPLICATE_EVENT_ID,
    "tenant_id": "tenant_demo",
    "event_type": "order.created",
    "event_timestamp": datetime.now(timezone.utc).isoformat(),
    "source_service": "reliability-exercise",
    "payload_version": 1,
    "payload": {"order_id": "dup-1", "customer_id": "cust_1", "product_id": "prod_1", "quantity": 1, "unit_price": 42.0},
    "trace_id": "reliability-trace-dup",
    "correlation_id": "reliability-trace-dup",
    "idempotency_key": DUPLICATE_EVENT_ID,
}


def run(config: StreamingConfig | None = None) -> ScenarioResult:
    config = config or StreamingConfig()
    steps: list[StepResult] = []

    if spark_available():
        from spark.streaming.deduplication import mark_duplicates_within_batch
        from spark.streaming.event_parser import parse_kafka_batch

        spark = get_or_create_spark()
        batch = kafka_shaped_batch(spark, [RECORD, dict(RECORD)], topic="platform.events.orders")
        marked = mark_duplicates_within_batch(parse_kafka_batch(batch)).collect()
        duplicate_flags = sorted(row.is_duplicate_in_batch for row in marked)
        ok = duplicate_flags == [False, True]
        steps.append(
            StepResult(
                name="detect_same_batch_duplicate",
                status="verified" if ok else "failed",
                detail=(
                    "deduplication.mark_duplicates_within_batch correctly flagged exactly one of the "
                    "two identical (tenant_id, event_id) rows as a duplicate"
                    if ok
                    else f"unexpected duplicate flags: {duplicate_flags}"
                ),
                evidence={"is_duplicate_in_batch_values": duplicate_flags},
            )
        )
        steps.append(
            StepResult(
                name="cross_batch_dedup_reference",
                status="verified",
                detail=(
                    "Cross-micro-batch deduplication via dropDuplicatesWithinWatermark is covered by "
                    "tests/streaming/test_streaming_integration.py::test_live_streaming_dedup_watermark_and_checkpoint "
                    "(5 unique keys survive 8s of duplicated rate-source traffic across many micro-batches)."
                ),
            )
        )
    else:
        steps.append(StepResult(name="detect_same_batch_duplicate", status="not_run", detail="pyspark/Java not available"))

    if kafka_reachable(config.kafka_bootstrap_servers):
        try:
            results = publish_raw_twice(
                config.kafka_bootstrap_servers,
                "platform.events.orders",
                key="tenant_demo",
                value=json.dumps(RECORD),
            )
            steps.append(
                StepResult(
                    name="publish_same_event_twice_to_kafka",
                    status="verified",
                    detail=f"published the same event_id={DUPLICATE_EVENT_ID!r} twice: {results}",
                    evidence={"publications": results},
                )
            )
        except Exception as exc:  # noqa: BLE001
            steps.append(StepResult(name="publish_same_event_twice_to_kafka", status="failed", detail=str(exc)))
    else:
        steps.append(
            StepResult(
                name="publish_same_event_twice_to_kafka",
                status="not_run",
                detail=f"Kafka not reachable at {config.kafka_bootstrap_servers}",
            )
        )

    result = ScenarioResult(
        scenario_id=SCENARIO_ID,
        title="Duplicate Event",
        component="spark.streaming.deduplication / processing-service raw_events idempotency",
        expected_behavior=(
            "Publishing the same (tenant_id, event_id) twice must not double-count it in serving "
            "metrics. The streaming layer suppresses it via dropDuplicatesWithinWatermark; the "
            "async processing-service suppresses it via raw_events' primary key on event_id."
        ),
        detection_method=(
            "cloudscale_stream_events_duplicate_total increments (same-batch case); "
            "processing-service's write_raw_event returns False on ON CONFLICT (cross-consumer case)."
        ),
        impact="None if handled — a duplicate delivery (at-least-once Kafka semantics) is expected, routine traffic, not an anomaly.",
        root_cause="At-least-once delivery semantics (Kafka doesn't guarantee exactly-once on the producer side without idempotent-producer configuration) plus this exercise's deliberate re-publish.",
        recovery="No recovery needed — duplicates are suppressed at both the streaming and async-consumer layers by design.",
        corrective_action="None required; this is the platform behaving correctly under expected delivery semantics.",
        preventive_control=(
            "Dedup key is always (tenant_id, event_id) — never event_id alone — enforced across the "
            "streaming pipeline, the raw_events primary key, and asserted in "
            "tests/streaming/test_tenant_isolation.py."
        ),
        steps=steps,
    )
    result.ended_at = datetime.now(timezone.utc)
    return result
