"""Late-arriving-event reliability exercise.

Injects one moderately-late event (should still aggregate, flagged
`late_accepted`) and one excessively-late event (should be excluded from
aggregation, flagged `late_rejected` and routed toward DLQ/audit) through
the real `spark.streaming.watermarking` classification code.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from reliability.injectors.spark_harness import (
    get_or_create_spark,
    kafka_shaped_batch,
    spark_available,
)
from reliability.models import ScenarioResult, StepResult
from spark.streaming.config import StreamingConfig

SCENARIO_ID = "late-event"


def _record(event_id: str, seconds_late: int) -> dict:
    ts = (datetime.now(timezone.utc) - timedelta(seconds=seconds_late)).isoformat()
    return {
        "event_id": event_id,
        "tenant_id": "tenant_demo",
        "event_type": "order.created",
        "event_timestamp": ts,
        "source_service": "reliability-exercise",
        "payload_version": 1,
        "payload": {"order_id": event_id, "customer_id": "cust_1", "product_id": "prod_1", "quantity": 1, "unit_price": 10.0},
        "trace_id": f"reliability-trace-{event_id}",
        "correlation_id": f"reliability-trace-{event_id}",
        "idempotency_key": event_id,
    }


def run(config: StreamingConfig | None = None) -> ScenarioResult:
    config = config or StreamingConfig()
    steps: list[StepResult] = []

    if spark_available():
        from spark.streaming.event_parser import parse_kafka_batch
        from spark.streaming.validation import split_valid_invalid, validate_events
        from spark.streaming.watermarking import classify_lateness, split_by_lateness

        spark = get_or_create_spark()
        moderately_late = _record("reliability-late-accepted", config.late_accept_threshold_seconds + 30)
        excessively_late = _record("reliability-late-rejected", config.late_reject_threshold_seconds + 300)

        batch = kafka_shaped_batch(spark, [moderately_late, excessively_late], topic="platform.events.orders")
        validated = validate_events(parse_kafka_batch(batch))
        valid, _invalid = split_valid_invalid(validated)
        classified = classify_lateness(valid, config)
        aggregatable, rejected = split_by_lateness(classified)

        aggregatable_ids = {row.event_id for row in aggregatable.collect()}
        rejected_ids = {row.event_id for row in rejected.collect()}

        ok = aggregatable_ids == {"reliability-late-accepted"} and rejected_ids == {"reliability-late-rejected"}
        steps.append(
            StepResult(
                name="classify_and_split_late_events",
                status="verified" if ok else "failed",
                detail=(
                    "late_accepted event stayed in the aggregatable set and late_rejected event was "
                    "excluded, as expected"
                    if ok
                    else f"unexpected split: aggregatable={aggregatable_ids}, rejected={rejected_ids}"
                ),
                evidence={"aggregatable_event_ids": sorted(aggregatable_ids), "rejected_event_ids": sorted(rejected_ids)},
            )
        )
    else:
        steps.append(StepResult(name="classify_and_split_late_events", status="not_run", detail="pyspark/Java not available"))

    result = ScenarioResult(
        scenario_id=SCENARIO_ID,
        title="Late-Arriving Event",
        component="spark.streaming.watermarking",
        expected_behavior=(
            "An event moderately later than STREAMING_LATE_ACCEPT_THRESHOLD_SECONDS but within "
            "STREAMING_LATE_REJECT_THRESHOLD_SECONDS (<= the watermark) is classified late_accepted "
            "and still contributes to its aggregation window. An event past the reject threshold is "
            "classified late_rejected, excluded from aggregation, and routed to "
            "streaming_late_events + the DLQ for audit — never silently dropped."
        ),
        detection_method="lateness_classification column + cloudscale_stream_events_late_total{classification=...}.",
        impact="Late-accepted events slightly widen a window's effective lateness tolerance; late-rejected events are excluded from that window's aggregate but retained for reconciliation.",
        root_cause="Clock skew, network delay, or an upstream retry/backfill delivering an event well after its event_timestamp.",
        recovery="late_rejected events remain queryable in streaming_late_events for manual reconciliation or backfill via scripts/backfill_metrics.py.",
        corrective_action="If late-rejected volume is consistently high for a tenant/source, widen STREAMING_WATERMARK_DELAY (and the matching late_reject threshold) for that pipeline.",
        preventive_control="config.py::validate_config enforces late_reject_threshold_seconds <= watermark_delay so the classification can never contradict what Spark's watermark actually does.",
        steps=steps,
    )
    result.ended_at = datetime.now(timezone.utc)
    return result
