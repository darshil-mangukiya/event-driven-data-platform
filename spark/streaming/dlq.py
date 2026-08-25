"""Route invalid/rejected events to the Kafka DLQ topic.

Uses Spark's batch (non-streaming) Kafka writer — ``df.write.format("kafka")``
— called from inside ``foreachBatch`` on the already-materialized
micro-batch DataFrame. This reuses the existing
``platform.events.dlq`` topic (see ``kafka/topics.yaml``) rather than
inventing a second dead-letter mechanism; the DLQ payload wraps the
original raw Kafka value together with why it was rejected, so
``scripts/`` DLQ-replay tooling and the ops console's existing DLQ views
keep working against the same topic.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.streaming.config import StreamingConfig


def build_dlq_payload(invalid_df: DataFrame) -> DataFrame:
    """Shape invalid/rejected rows into a Kafka key/value payload for the DLQ topic."""
    dlq_envelope = F.to_json(
        F.struct(
            F.col("event_id").alias("original_event_id"),
            F.col("tenant_id"),
            F.col("event_type"),
            F.col("source_service"),
            F.coalesce(F.col("validation_reason"), F.lit("late_rejected")).alias("rejection_reason"),
            F.col("kafka_topic").alias("original_topic"),
            F.col("kafka_partition").alias("original_partition"),
            F.col("kafka_offset").alias("original_offset"),
            F.col("raw_value"),
            F.current_timestamp().cast("string").alias("dlq_written_at"),
        )
    )
    key = F.coalesce(F.col("tenant_id"), F.lit("unknown")).cast("string")
    return invalid_df.select(key.alias("key"), dlq_envelope.alias("value"))


def write_to_dlq(invalid_df: DataFrame, config: StreamingConfig) -> int:
    """Batch-write rejected rows to the DLQ topic. Returns the row count written."""
    if invalid_df.isEmpty():
        return 0
    payload = build_dlq_payload(invalid_df)
    row_count = payload.count()
    (
        payload.write.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("topic", config.dlq_topic)
        .save()
    )
    return row_count
