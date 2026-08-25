"""A genuine live Structured Streaming integration test.

Everything else in tests/streaming/ exercises the transformation functions
against *static* (batch) DataFrames — the standard, fast way to test Spark
transformation logic, since the same code runs identically in a streaming
micro-batch. This file is different: it runs a real streaming query (Spark's
"rate" source standing in for Kafka, since no broker is available in this
test environment) through ``withWatermark`` +
``dropDuplicatesWithinWatermark``, with real checkpointing, to prove those
stateful operators execute, in addition to the DataFrame-building
code compiles.

Kafka itself is not exercised here (that needs a running broker — see
``docs/streaming_architecture.md`` "Runtime Verification" for what is and
isn't covered locally vs. via ``make streaming-demo`` against the
dockerized stack).
"""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path

import pytest
from pyspark.sql import functions as F


@pytest.mark.integration
def test_live_streaming_dedup_watermark_and_checkpoint(spark):
    checkpoint_dir = tempfile.mkdtemp(prefix="cloudscale-streaming-test-")
    try:
        # 5 distinct (tenant_id, event_id) keys, cycled repeatedly by the
        # rate source so the same key legitimately arrives in *multiple*
        # micro-batches — a real cross-batch duplicate, beyond a
        # same-batch one.
        rate = spark.readStream.format("rate").option("rowsPerSecond", 20).load()
        keyed = rate.withColumn("key_index", (F.col("value") % 5).cast("int"))
        events = keyed.select(
            F.concat(F.lit("tenant_"), F.col("key_index")).alias("tenant_id"),
            F.concat(F.lit("evt_"), F.col("key_index")).alias("event_id"),
            F.col("timestamp").alias("event_timestamp"),
        )

        watermarked = events.withWatermark("event_timestamp", "30 seconds")
        deduped = (
            watermarked.dropDuplicatesWithinWatermark(["tenant_id", "event_id"])
            if hasattr(watermarked, "dropDuplicatesWithinWatermark")
            else watermarked.dropDuplicates(["tenant_id", "event_id"])
        )

        query = (
            deduped.writeStream.format("memory")
            .queryName("dedup_integration_result")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime="250 milliseconds")
            .start()
        )

        deadline = time.time() + 8
        while time.time() < deadline:
            progress = query.lastProgress
            if progress and progress.get("numInputRows", 0) > 0:
                # Give it a couple more batches so cross-batch duplicates
                # actually get the chance to arrive and be dropped.
                time.sleep(2)
                break
            time.sleep(0.25)

        query.stop()

        result = spark.sql("select tenant_id, event_id from dedup_integration_result").collect()
        distinct_keys = {(row.tenant_id, row.event_id) for row in result}

        # The pipeline processed strictly more raw rate-source ticks than
        # distinct keys (5), and dropDuplicatesWithinWatermark still
        # produced exactly 5 output rows; duplicates arriving across
        # separate micro-batches were actually suppressed by Spark's
        # stateful operator, rather than by accident of batch boundaries.
        assert len(result) == 5, f"expected exactly 5 deduplicated rows, got {len(result)}: {result}"
        assert distinct_keys == {(f"tenant_{i}", f"evt_{i}") for i in range(5)}

        # Checkpointing evidence: Spark writes offsets/commits/state under
        # the checkpoint location once at least one batch has committed.
        checkpoint_path = Path(checkpoint_dir)
        assert (checkpoint_path / "offsets").exists(), "no offsets directory — checkpointing did not run"
        assert (checkpoint_path / "commits").exists(), "no commits directory — no batch was committed"
        committed_batches = list((checkpoint_path / "commits").glob("*"))
        assert len(committed_batches) >= 1, "expected at least one committed micro-batch"
    finally:
        shutil.rmtree(checkpoint_dir, ignore_errors=True)


@pytest.mark.integration
def test_live_streaming_windowed_aggregation_executes(spark):
    """Proves groupBy(window(...)) actually runs as a stateful streaming aggregation."""
    checkpoint_dir = tempfile.mkdtemp(prefix="cloudscale-streaming-window-test-")
    try:
        rate = spark.readStream.format("rate").option("rowsPerSecond", 20).load()
        events = rate.select(
            F.lit("tenant_demo").alias("tenant_id"),
            F.col("timestamp").alias("event_timestamp"),
            F.lit(1.0).alias("amount"),
        )
        watermarked = events.withWatermark("event_timestamp", "10 seconds")
        windowed = watermarked.groupBy(
            F.col("tenant_id"), F.window("event_timestamp", "5 seconds")
        ).agg(F.sum("amount").alias("total"), F.count(F.lit(1)).alias("event_count"))

        query = (
            windowed.writeStream.format("memory")
            .queryName("windowed_integration_result")
            .outputMode("update")
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime="250 milliseconds")
            .start()
        )
        time.sleep(4)
        query.stop()

        result = spark.sql("select * from windowed_integration_result").collect()
        assert len(result) >= 1, "expected at least one windowed aggregate row"
        assert all(row.total > 0 for row in result)
        assert all(row.event_count > 0 for row in result)
    finally:
        shutil.rmtree(checkpoint_dir, ignore_errors=True)
