"""Event-time watermarking and late-arriving-event classification.

Spark's watermark (``withWatermark``) is what bounds state size for the
deduplication and windowed-aggregation stages: it tells Spark "I don't
expect events more than N late relative to the max event time seen so far,
so it's safe to drop state older than that." Rows older than the watermark
are dropped by Spark itself, silently, once they reach a stateful operator
(dedup / groupBy-window) — that silent-drop behavior is exactly what the
platform requirements reject unnoticed late-data loss.

So this module does two distinct things:

1. :func:`apply_watermark` — set the actual Spark watermark used by the
   downstream stateful operators (dedup, windowed aggregation).
2. :func:`classify_lateness` — an *application-level* estimate of how late
   each event is (``current_timestamp() - event_timestamp``), independent
   of Spark's internal watermark value (which isn't exposed as a row-level
   column). This estimate is what produces the ``lateness_classification``
   used for metrics and for routing "too late to matter" events to the
   audit table before they'd otherwise vanish into Spark's watermark drop.

Processing-time vs. event-time: ``event_timestamp`` on every row is the
producer-supplied event time (when the business event happened);
``kafka_ingest_timestamp`` is when Kafka received the record;
``current_timestamp()`` at classification time is processing time. All
three are kept as separate columns so lag between them is directly
observable.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.streaming.config import StreamingConfig

ON_TIME = "on_time"
LATE_ACCEPTED = "late_accepted"
LATE_REJECTED = "late_rejected"


def apply_watermark(df: DataFrame, config: StreamingConfig) -> DataFrame:
    """Set the Spark watermark on ``event_timestamp``.

    Must be called before :func:`spark.streaming.deduplication.deduplicate`
    and before any ``groupBy(window(...))`` aggregation — Spark only honors
    a watermark declared upstream of the stateful operator that uses it.
    """
    return df.withWatermark("event_timestamp", config.watermark_delay)


def classify_lateness(df: DataFrame, config: StreamingConfig) -> DataFrame:
    """Add ``lateness_seconds`` and ``lateness_classification`` columns.

    Only meaningful for rows that already passed structural/contract
    validation and have a non-null ``event_timestamp`` — invalid rows are
    left with a null classification since they're routed to DLQ separately.
    """
    lateness_seconds = F.when(
        F.col("event_timestamp").isNotNull(),
        F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("event_timestamp")),
    )
    df = df.withColumn("lateness_seconds", lateness_seconds)
    df = df.withColumn(
        "lateness_classification",
        F.when(F.col("lateness_seconds").isNull(), F.lit(None).cast("string"))
        .when(F.col("lateness_seconds") <= F.lit(config.late_accept_threshold_seconds), F.lit(ON_TIME))
        .when(F.col("lateness_seconds") <= F.lit(config.late_reject_threshold_seconds), F.lit(LATE_ACCEPTED))
        .otherwise(F.lit(LATE_REJECTED)),
    )
    return df


def split_by_lateness(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (aggregatable, rejected) — on_time + late_accepted vs late_rejected."""
    aggregatable = df.filter(F.col("lateness_classification") != LATE_REJECTED)
    rejected = df.filter(F.col("lateness_classification") == LATE_REJECTED)
    return aggregatable, rejected
