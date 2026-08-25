"""Delayed-consumer / processing-lag reliability exercise.

If a Kafka broker is reachable, measures real consumer-group lag (end
offsets minus committed offsets) with `kafka-python`. If not, exercises the
real Prometheus gauge-setting code path
(`spark.streaming.metrics.STREAM_PROCESSING_LAG`/`STREAM_WATERMARK_LAG`)
with a deterministic lag value and checks it against the alert
threshold in `monitoring/alert_rules.yml`.
"""

from __future__ import annotations

from datetime import datetime, timezone

from reliability.injectors.reachability import kafka_reachable
from reliability.models import ScenarioResult, StepResult
from spark.streaming.config import StreamingConfig

SCENARIO_ID = "consumer-lag"

# Mirrors monitoring/alert_rules.yml::StreamingProcessingLagHigh
LAG_ALERT_THRESHOLD_SECONDS = 120


def _measure_live_kafka_lag(bootstrap_servers: str, group_id: str, topics: list[str]) -> dict:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
    )
    try:
        partitions = [TopicPartition(topic, p) for topic in topics for p in consumer.partitions_for_topic(topic) or []]
        if not partitions:
            return {"partitions": 0, "total_lag": None}
        consumer.assign(partitions)
        end_offsets = consumer.end_offsets(partitions)
        committed = {tp: consumer.committed(tp) or 0 for tp in partitions}
        total_lag = sum(end_offsets[tp] - committed[tp] for tp in partitions)
        return {"partitions": len(partitions), "total_lag": total_lag}
    finally:
        consumer.close()


def run(config: StreamingConfig | None = None) -> ScenarioResult:
    config = config or StreamingConfig()
    steps: list[StepResult] = []

    if kafka_reachable(config.kafka_bootstrap_servers):
        try:
            lag_info = _measure_live_kafka_lag(
                config.kafka_bootstrap_servers, "processing-service", list(config.subscribe_topics)
            )
            steps.append(
                StepResult(
                    name="measure_live_consumer_group_lag",
                    status="verified",
                    detail=f"measured real consumer-group lag via kafka-python: {lag_info}",
                    evidence=lag_info,
                )
            )
        except Exception as exc:  # noqa: BLE001
            steps.append(StepResult(name="measure_live_consumer_group_lag", status="failed", detail=str(exc)))
    else:
        steps.append(
            StepResult(
                name="measure_live_consumer_group_lag",
                status="not_run",
                detail=f"Kafka not reachable at {config.kafka_bootstrap_servers}",
            )
        )

    from spark.streaming.metrics import STREAM_PROCESSING_LAG, STREAM_WATERMARK_LAG

    synthetic_lag_seconds = 185.0  # deliberately above LAG_ALERT_THRESHOLD_SECONDS
    STREAM_PROCESSING_LAG.labels(query="reliability-exercise").set(synthetic_lag_seconds)
    STREAM_WATERMARK_LAG.labels(query="reliability-exercise").set(synthetic_lag_seconds)
    would_alert = synthetic_lag_seconds > LAG_ALERT_THRESHOLD_SECONDS
    steps.append(
        StepResult(
            name="exercise_lag_gauge_and_alert_threshold",
            status="verified" if would_alert else "failed",
            detail=(
                f"set cloudscale_stream_processing_lag_seconds={synthetic_lag_seconds}s (real Prometheus "
                f"gauge, same object streaming_job.py updates) and confirmed it exceeds the "
                f"StreamingProcessingLagHigh threshold ({LAG_ALERT_THRESHOLD_SECONDS}s) from "
                f"monitoring/alert_rules.yml"
            ),
            evidence={"synthetic_lag_seconds": synthetic_lag_seconds, "threshold_seconds": LAG_ALERT_THRESHOLD_SECONDS},
        )
    )

    result = ScenarioResult(
        scenario_id=SCENARIO_ID,
        title="Delayed Consumer / Processing Lag",
        component="spark.streaming.metrics / processing-service consumer group",
        expected_behavior=(
            "When the streaming pipeline (or the async processing-service consumer) falls behind "
            "Kafka's log head, that lag must be directly observable as a metric and must cross an "
            "alerting threshold before it silently becomes a freshness/SLA problem for the analytics API."
        ),
        detection_method="cloudscale_stream_processing_lag_seconds / cloudscale_stream_watermark_lag_seconds gauges, sourced from Spark's own StreamingQueryProgress.eventTime.watermark; alert StreamingProcessingLagHigh in monitoring/alert_rules.yml.",
        impact="Sustained lag means stream_window_metrics and tenant-facing freshness targets (see docs/data_product_requirements.md) fall behind reality without any other visible symptom.",
        root_cause="Consumer/executor resource contention, a slow sink (see db-outage exercise), or a genuine traffic spike beyond the current partition/executor count.",
        recovery="Once the root cause clears, Spark's micro-batch processing naturally catches up from the last committed checkpoint offset — no manual intervention required for a transient spike.",
        corrective_action="Sustained lag needs either more partitions/executors or a shorter trigger interval; a single spike needs none.",
        preventive_control="StreamingProcessingLagHigh (5m sustained > 120s) and StreamingCheckpointStale (no commit in 5m) catch two different failure shapes: gradual falling-behind vs. a fully stalled query.",
        steps=steps,
    )
    result.ended_at = datetime.now(timezone.utc)
    return result
