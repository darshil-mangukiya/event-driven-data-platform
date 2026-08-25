"""Streaming deduplication on (tenant_id, event_id).

Two complementary mechanisms are used, deliberately:

1. :func:`deduplicate` — the real, stateful, watermark-scoped Spark
   operator (``dropDuplicatesWithinWatermark`` on Spark >= 3.5, falling back
   to plain ``dropDuplicates`` on older runtimes with a logged warning).
   This is what actually protects the pipeline: it bounds the state Spark
   keeps for "have I seen this key before" to the watermark window, so
   state doesn't grow unbounded, and it drops true duplicates before they
   ever reach the aggregation stage.

2. :func:`mark_duplicates_within_batch` — a per-micro-batch, row_number-based
   duplicate *counter* used purely for observability
   (``cloudscale_stream_events_duplicate_total`` and the
   ``streaming_late_events``/metrics tables). It only sees duplicates that
   land in the same micro-batch; a duplicate arriving in a later batch is
   still correctly suppressed by (1), it just isn't separately counted by
   (2). That tradeoff is documented in docs/streaming_architecture.md —
   Spark's Structured Streaming progress API does not expose a clean
   "rows dropped by dropDuplicatesWithinWatermark" counter, so an exact
   cross-batch duplicate count is not available without maintaining a
   second, redundant idempotency ledger table, which we chose not to add.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)

DEDUP_KEYS = ("tenant_id", "event_id")


def deduplicate(df: DataFrame) -> DataFrame:
    """Apply the stateful, watermark-scoped dedup operator.

    Must be called after :func:`spark.streaming.watermarking.apply_watermark`
    — ``dropDuplicatesWithinWatermark`` requires a watermark to already be
    defined on the DataFrame, that's what bounds its state.
    """
    if hasattr(df, "dropDuplicatesWithinWatermark"):
        return df.dropDuplicatesWithinWatermark(list(DEDUP_KEYS))
    LOGGER.warning(
        "Spark runtime does not support dropDuplicatesWithinWatermark; "
        "falling back to unbounded-state dropDuplicates(%s)",
        DEDUP_KEYS,
    )
    return df.dropDuplicates(list(DEDUP_KEYS))


def mark_duplicates_within_batch(df: DataFrame) -> DataFrame:
    """Add ``is_duplicate_in_batch`` for rows sharing a (tenant_id, event_id).

    Intended for use on a *static* micro-batch DataFrame (e.g. inside
    ``foreachBatch``), not on the live streaming DataFrame — ``Window``
    without a partition-scoped watermark is only safe on bounded data.
    """
    window = Window.partitionBy(*DEDUP_KEYS).orderBy(F.col("kafka_offset").asc_nulls_last())
    ranked = df.withColumn("_dedup_rank", F.row_number().over(window))
    ranked = ranked.withColumn("is_duplicate_in_batch", F.col("_dedup_rank") > 1)
    return ranked.drop("_dedup_rank")
