"""Entrypoint: Kafka -> Spark Structured Streaming -> PostgreSQL.

Wires the pipeline modules together into three independent streaming
queries, each with its own checkpoint directory so they can be
independently restarted/recovered without colliding:

* **cloudscale-stream-aggregates** — the main business path. Parses,
  validates, watermarks, deduplicates, classifies lateness, enriches with
  tenant metadata, computes windowed revenue/orders/payment-health/
  throughput, and upserts into ``stream_window_metrics``.
* **cloudscale-stream-dlq** — invalid (contract-breaking) events and
  too-late-to-aggregate events, written to the Kafka DLQ topic and audited
  into ``streaming_late_events``.
* **cloudscale-stream-dedup-audit** — sees events *before* Spark's
  watermark-scoped dedup operator runs, so it can observe and count
  same-micro-batch duplicates for the ``cloudscale_stream_events_duplicate_total``
  metric (see ``deduplication.py`` for why cross-batch duplicates aren't
  separately counted here).

Run locally with Spark installed:

    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \\
        spark/streaming/streaming_job.py

Or via the Spark cluster defined in docker-compose.yml (see
``make streaming-demo``).
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import sys
import time
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.streaming.aggregations import build_window_aggregates, to_long_format
from spark.streaming.config import StreamingConfig, validate_config
from spark.streaming.deduplication import deduplicate, mark_duplicates_within_batch
from spark.streaming.dlq import write_to_dlq
from spark.streaming.enrichment import enrich_with_tenant_metadata, load_tenant_metadata
from spark.streaming.event_parser import parse_kafka_batch
from spark.streaming.kafka_source import build_kafka_read_stream
from spark.streaming.metrics import (
    STREAM_BATCHES_TOTAL,
    STREAM_DLQ_TOTAL,
    STREAM_EVENTS_DUPLICATE,
    STREAM_EVENTS_FAILED,
    STREAM_EVENTS_PROCESSED,
    STREAM_EVENTS_RECEIVED,
    STREAM_PROCESSING_LAG,
    STREAM_WATERMARK_LAG,
    start_metrics_server,
)
from spark.streaming.sinks import PostgresSink
from spark.streaming.validation import split_valid_invalid, validate_events
from spark.streaming.watermarking import apply_watermark, classify_lateness, split_by_lateness

LOGGER = logging.getLogger("cloudscale.streaming_job")


def build_spark_session(config: StreamingConfig) -> SparkSession:
    return (
        SparkSession.builder.appName(config.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )


def run(spark: SparkSession, config: StreamingConfig) -> dict[str, object]:
    """Start all five streaming queries. Returns the started queries by name."""
    sink = PostgresSink(config, run_id=str(uuid.uuid4()))
    sink.start_run("cloudscale-structured-streaming", dataclasses.asdict(config))
    start_metrics_server(config.metrics_port)

    # Populated with the query object right after each .start() call below.
    # foreachBatch callbacks close over this dict (not the query variable
    # itself, which doesn't exist yet when the closure is defined) so they
    # can read query.lastProgress — including Spark's own event-time
    # watermark — once at least one batch has run.
    query_refs: dict[str, object] = {}

    tenant_metadata = load_tenant_metadata(spark, config)

    raw = build_kafka_read_stream(spark, config)
    parsed = parse_kafka_batch(raw)
    if config.tenant_filter:
        parsed = parsed.filter(F.col("tenant_id").isin(*config.tenant_filter))
    validated = validate_events(parsed)
    valid, invalid = split_valid_invalid(validated)

    watermarked_valid = apply_watermark(valid, config)

    # -- Query: dedup-audit (sees pre-dedup data, purely for observability)
    def dedup_audit_batch(batch_df, batch_id: int) -> None:
        start = time.monotonic()
        try:
            marked = mark_duplicates_within_batch(batch_df)
            duplicates = marked.filter(F.col("is_duplicate_in_batch"))
            dup_rows = duplicates.select(
                "event_id",
                "tenant_id",
                "event_type",
                "event_domain",
                "event_timestamp",
                F.col("kafka_ingest_timestamp"),
                F.lit(None).cast("bigint").alias("lateness_seconds"),
                F.lit("duplicate").alias("lateness_classification"),
            )
            written = sink.write_late_events(dup_rows, batch_id)
            if written:
                STREAM_EVENTS_DUPLICATE.labels(event_domain="all").inc(written)
            sink.write_checkpoint_audit(
                query_name="dedup-audit",
                batch_id=batch_id,
                checkpoint_location=config.checkpoint_path("dedup-audit"),
                input_rows=batch_df.count(),
                batch_duration_ms=(time.monotonic() - start) * 1000,
            )
            STREAM_BATCHES_TOTAL.labels(query="dedup-audit", outcome="success").inc()
        except Exception as exc:  # noqa: BLE001
            sink.log_failure(batch_id=batch_id, tenant_id=None, stage="dedup-audit", error_message=str(exc))
            STREAM_BATCHES_TOTAL.labels(query="dedup-audit", outcome="failed").inc()
            raise

    dedup_audit_query = (
        watermarked_valid.writeStream.foreachBatch(dedup_audit_batch)
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint_path("dedup-audit"))
        .trigger(processingTime=config.trigger_processing_time)
        .queryName("cloudscale-stream-dedup-audit")
        .start()
    )

    # -- Main path: dedup -> classify -> split -> enrich -> aggregate
    deduped = deduplicate(watermarked_valid)
    classified = classify_lateness(deduped, config)
    aggregatable, late_rejected = split_by_lateness(classified)

    enriched = enrich_with_tenant_metadata(aggregatable, tenant_metadata)
    aggregated = build_window_aggregates(enriched, config)
    long_format = to_long_format(aggregated)

    def aggregates_batch(batch_df, batch_id: int) -> None:
        start = time.monotonic()
        try:
            written = sink.write_window_metrics(batch_df, batch_id)
            for row in batch_df.filter(F.col("metric_name") == "event_count").collect():
                STREAM_EVENTS_PROCESSED.labels(event_domain=row["event_domain"]).inc(row["metric_value"] or 0)

            # Real lag/watermark metrics, sourced from the query's own
            # progress report (Spark's actual internal watermark, not an
            # application-level guess) — see query_refs above.
            query_obj = query_refs.get("aggregates")
            progress = getattr(query_obj, "lastProgress", None) if query_obj is not None else None
            if progress:
                event_time = progress.get("eventTime") or {}
                watermark_str = event_time.get("watermark")
                if watermark_str:
                    watermark_dt = datetime.strptime(watermark_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                    now = datetime.utcnow()
                    watermark_lag_seconds = max((now - watermark_dt).total_seconds(), 0.0)
                    STREAM_WATERMARK_LAG.labels(query="aggregates").set(watermark_lag_seconds)
                    sink.write_watermark(batch_id=batch_id, event_time_watermark=watermark_dt)
                batch_duration_progress_ms = progress.get("batchDuration")
                if batch_duration_progress_ms is not None:
                    # Spark's own measured batch duration is a more direct
                    # processing-lag signal than a window-boundary estimate.
                    STREAM_PROCESSING_LAG.labels(query="aggregates").set(batch_duration_progress_ms / 1000.0)

            sink.write_checkpoint_audit(
                query_name="aggregates",
                batch_id=batch_id,
                checkpoint_location=config.checkpoint_path("aggregates"),
                input_rows=written,
                batch_duration_ms=(time.monotonic() - start) * 1000,
            )
            STREAM_BATCHES_TOTAL.labels(query="aggregates", outcome="success").inc()
        except Exception as exc:  # noqa: BLE001
            sink.log_failure(batch_id=batch_id, tenant_id=None, stage="write_window_metrics", error_message=str(exc))
            STREAM_BATCHES_TOTAL.labels(query="aggregates", outcome="failed").inc()
            raise

    aggregates_query = (
        long_format.writeStream.foreachBatch(aggregates_batch)
        .outputMode(config.output_mode_aggregates)
        .option("checkpointLocation", config.checkpoint_path("aggregates"))
        .trigger(processingTime=config.trigger_processing_time)
        .queryName("cloudscale-stream-aggregates")
        .start()
    )
    query_refs["aggregates"] = aggregates_query

    # Late-accepted rows still get an audit trail even though they also feed
    # aggregation (their existence, not their absence, is the signal here).
    late_accepted_events = aggregatable.filter(F.col("lateness_classification") == "late_accepted")

    def late_accepted_batch(batch_df, batch_id: int) -> None:
        try:
            sink.write_late_events(batch_df, batch_id)
        except Exception as exc:  # noqa: BLE001
            sink.log_failure(batch_id=batch_id, tenant_id=None, stage="late_accepted_audit", error_message=str(exc))
            raise

    late_accepted_query = (
        late_accepted_events.select(
            "event_id",
            "tenant_id",
            "event_type",
            "event_domain",
            "event_timestamp",
            "kafka_ingest_timestamp",
            "lateness_seconds",
            "lateness_classification",
        )
        .writeStream.foreachBatch(late_accepted_batch)
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint_path("late-accepted-audit"))
        .trigger(processingTime=config.trigger_processing_time)
        .queryName("cloudscale-stream-late-accepted-audit")
        .start()
    )

    # -- DLQ path: contract-invalid events + too-late-to-matter events
    invalid_for_dlq = invalid.withColumn("lateness_classification", F.lit(None).cast("string"))
    rejected_for_dlq = late_rejected.withColumn(
        "validation_reason", F.lit("event_time_watermark_exceeded")
    )
    dlq_source = invalid_for_dlq.unionByName(rejected_for_dlq, allowMissingColumns=True)

    def dlq_batch(batch_df, batch_id: int) -> None:
        start = time.monotonic()
        try:
            written = write_to_dlq(batch_df, config)
            reason_counts = batch_df.groupBy("validation_reason").count().collect()
            for row in reason_counts:
                reason = row["validation_reason"] or "unknown"
                STREAM_EVENTS_FAILED.labels(reason=reason).inc(row["count"])
                STREAM_DLQ_TOTAL.labels(reason=reason).inc(row["count"])
            late_rows = batch_df.filter(F.col("validation_reason") == "event_time_watermark_exceeded").select(
                "event_id",
                "tenant_id",
                "event_type",
                "event_domain",
                "event_timestamp",
                "kafka_ingest_timestamp",
                "lateness_seconds",
                F.lit("late_rejected").alias("lateness_classification"),
            )
            sink.write_late_events(late_rows, batch_id)
            sink.write_checkpoint_audit(
                query_name="dlq",
                batch_id=batch_id,
                checkpoint_location=config.checkpoint_path("dlq"),
                input_rows=written,
                batch_duration_ms=(time.monotonic() - start) * 1000,
            )
            STREAM_BATCHES_TOTAL.labels(query="dlq", outcome="success").inc()
        except Exception as exc:  # noqa: BLE001
            sink.log_failure(batch_id=batch_id, tenant_id=None, stage="dlq", error_message=str(exc))
            STREAM_BATCHES_TOTAL.labels(query="dlq", outcome="failed").inc()
            raise

    dlq_query = (
        dlq_source.writeStream.foreachBatch(dlq_batch)
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint_path("dlq"))
        .trigger(processingTime=config.trigger_processing_time)
        .queryName("cloudscale-stream-dlq")
        .start()
    )

    def received_batch(batch_df, batch_id: int) -> None:
        for row in batch_df.groupBy("kafka_topic").count().collect():
            STREAM_EVENTS_RECEIVED.labels(topic=row["kafka_topic"]).inc(row["count"])

    received_query = (
        parsed.writeStream.foreachBatch(received_batch)
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint_path("received-metrics"))
        .trigger(processingTime=config.trigger_processing_time)
        .queryName("cloudscale-stream-received-metrics")
        .start()
    )

    return {
        "aggregates": aggregates_query,
        "dlq": dlq_query,
        "dedup_audit": dedup_audit_query,
        "late_accepted_audit": late_accepted_query,
        "received_metrics": received_query,
        "sink": sink,
    }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Event-Driven Data Platform Structured Streaming job")
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--checkpoint-root", default=None)
    parser.add_argument("--trigger-interval", default=None)
    parser.add_argument("--starting-offsets", default=None)
    parser.add_argument("--await-termination-seconds", type=float, default=None, help="Local/testing only: stop after N seconds instead of running forever.")
    args = parser.parse_args(argv)

    config = StreamingConfig.from_env()
    overrides = {}
    if args.bootstrap_servers:
        overrides["kafka_bootstrap_servers"] = args.bootstrap_servers
    if args.checkpoint_root:
        overrides["checkpoint_root"] = args.checkpoint_root
    if args.trigger_interval:
        overrides["trigger_processing_time"] = args.trigger_interval
    if args.starting_offsets:
        overrides["starting_offsets"] = args.starting_offsets
    if overrides:
        config = dataclasses.replace(config, **overrides)

    errors = validate_config(config)
    if errors:
        for error in errors:
            LOGGER.error("invalid streaming config: %s", error)
        return 1

    spark = build_spark_session(config)
    queries = run(spark, config)
    sink: PostgresSink = queries["sink"]  # type: ignore[assignment]

    try:
        if args.await_termination_seconds:
            time.sleep(args.await_termination_seconds)
            for name, query in queries.items():
                if name == "sink":
                    continue
                query.stop()
            sink.finish_run("completed")
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        LOGGER.info("shutdown requested")
        sink.finish_run("stopped")
    except Exception:
        LOGGER.exception("streaming job failed")
        sink.finish_run("failed")
        raise
    return 0


if __name__ == "__main__":
    sys.exit(main())
