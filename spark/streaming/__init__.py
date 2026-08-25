"""Spark Structured Streaming pipeline for the event-driven data platform.

This package turns the existing Kafka + PySpark foundation into a real
Structured Streaming pipeline:

    Kafka (readStream)
        -> event_parser (JSON -> typed columns, malformed record handling)
        -> validation (contract/business-rule validation, invalid routing)
        -> deduplication (tenant_id + event_id, watermark-aware)
        -> watermarking (event-time watermark, late-event classification)
        -> enrichment (broadcast tenant metadata)
        -> aggregations (windowed revenue / orders / payment health / throughput)
        -> sinks (foreachBatch upserts into PostgreSQL, DLQ to Kafka)

Every module exposes small, pure(ish) functions that operate on Spark
DataFrames so the transformation logic can be exercised with static
(batch) DataFrames in unit tests, and with real streaming queries in the
integration tests under ``tests/streaming/``.

Design note: this pipeline is additive. The existing async
``processing-service`` Kafka consumer remains the system of record for
row-level ``processed_orders`` / ``processed_payments`` /
``processed_user_sessions``. This Structured Streaming pipeline adds a
second, independent analytical path that demonstrates genuine event-time
processing (watermarks, late-data handling, windowed aggregation,
checkpointing) and writes to its own serving tables
(``stream_window_metrics``, ``streaming_late_events``, ...). See
docs/streaming_architecture.md for the full design and rationale.
"""
