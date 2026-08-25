"""Windowed streaming aggregations: revenue, orders, payment health, throughput.

Each aggregate answers a concrete SaaS/ecommerce question, matching the
metric requirements; no aggregation exists solely to demonstrate Spark
syntax:

* **revenue** — tenant + event-time window -> gross revenue (order line
  value for ``order.created``, captured/authorized payment amount for
  ``payment.captured``/``payment.authorized``).
* **order_count** / **units_sold** — tenant + window -> order volume.
* **payment_success_count** / **payment_failure_count** — tenant + window
  -> payment health, from which a failure rate is derivable downstream
  (kept as two counts rather than a pre-divided ratio so the serving layer
  can decide bucket sizes for the ratio without losing precision).
* **event_throughput** — tenant/domain + window -> event volume, the
  operational "is the pipeline keeping up" signal.

``build_window_aggregates`` operates on the *deduplicated, watermarked,
non-late-rejected* event stream (see ``streaming_job.py`` for pipeline
order) grouped by ``window(event_timestamp, window_duration)``. The result
is a wide DataFrame (one row per tenant+window+domain); ``to_long_format``
melts it into the tidy ``(metric_name, metric_value)`` shape that
``sinks.py`` upserts into ``stream_window_metrics`` — a single serving
table instead of one table per metric, per the "avoid unnecessary schema
bloat" guidance.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark.streaming.config import StreamingConfig

_NUMERIC_FIELDS = ("unit_price", "quantity", "amount")


def _payload_numeric(field_name: str) -> Column:
    return F.get_json_object(F.col("payload_json"), f"$.{field_name}").cast("double")


def build_window_aggregates(events_df: DataFrame, config: StreamingConfig) -> DataFrame:
    """Group deduplicated/watermarked events into per-tenant, per-domain windows."""
    with_metrics = events_df
    for field_name in _NUMERIC_FIELDS:
        with_metrics = with_metrics.withColumn(f"_{field_name}", _payload_numeric(field_name))

    with_metrics = with_metrics.withColumn(
        "_order_revenue",
        F.when(F.col("event_type") == "order.created", F.col("_unit_price") * F.coalesce(F.col("_quantity"), F.lit(1))),
    )
    with_metrics = with_metrics.withColumn(
        "_payment_revenue",
        F.when(F.col("event_type").isin("payment.authorized", "payment.captured"), F.col("_amount")),
    )

    window_col = (
        F.window("event_timestamp", config.window_duration, config.window_slide_duration)
        if config.window_slide_duration
        else F.window("event_timestamp", config.window_duration)
    )

    aggregated = (
        with_metrics.groupBy(F.col("tenant_id"), window_col.alias("window"), F.col("event_domain"))
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.sum(F.coalesce(F.col("_order_revenue"), F.lit(0.0))).alias("order_revenue"),
            F.sum(F.coalesce(F.col("_payment_revenue"), F.lit(0.0))).alias("payment_revenue"),
            F.sum(F.when(F.col("event_type") == "order.created", 1).otherwise(0)).alias("order_count"),
            F.sum(F.coalesce(F.when(F.col("event_type") == "order.created", F.col("_quantity")), F.lit(0))).alias(
                "units_sold"
            ),
            F.sum(F.when(F.col("event_type") == "payment.captured", 1).otherwise(0)).alias(
                "payment_success_count"
            ),
            F.sum(F.when(F.col("event_type") == "payment.failed", 1).otherwise(0)).alias(
                "payment_failure_count"
            ),
            F.sum(F.when(F.col("lateness_classification") == "late_accepted", 1).otherwise(0)).alias(
                "late_accepted_count"
            ),
        )
        .withColumn("revenue", F.col("order_revenue") + F.col("payment_revenue"))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window", "order_revenue", "payment_revenue")
    )
    return aggregated


_METRIC_COLUMNS = (
    "revenue",
    "order_count",
    "units_sold",
    "payment_success_count",
    "payment_failure_count",
    "event_count",
)


def to_long_format(aggregated_df: DataFrame) -> DataFrame:
    """Melt the wide window-aggregate DataFrame into tidy (metric_name, metric_value) rows."""
    parts = []
    for metric_name in _METRIC_COLUMNS:
        parts.append(
            aggregated_df.select(
                "tenant_id",
                "window_start",
                "window_end",
                "event_domain",
                F.lit(metric_name).alias("metric_name"),
                F.col(metric_name).cast("double").alias("metric_value"),
                "event_count",
            )
        )
    long_df = parts[0]
    for part in parts[1:]:
        long_df = long_df.unionByName(part)
    return long_df
