"""Broadcast enrichment of streaming events with tenant metadata.

Tradeoff: tenant metadata
(``tenant_config`` — plan, region, active flag) changes rarely relative to
event volume, and the table is small (one row per tenant). That makes it a
textbook case for a *broadcast* join: read it once per job run as a static
batch DataFrame, broadcast it to every executor, and join without a shuffle.

What this deliberately does NOT do: re-read ``tenant_config`` on every
micro-batch. A newly onboarded tenant or a plan change won't be reflected
in enrichment until the streaming job restarts. For a local/demo-scale
platform that's an acceptable tradeoff (documented in
docs/streaming_architecture.md); a production system would instead use a
periodically-refreshed broadcast (Spark's ``Trigger`` + a scheduled
re-broadcast, or a change-data-capture stream of ``tenant_config`` joined
as a second streaming source) — more machinery than this
local platform's tenant-change frequency justifies.

Product/domain metadata is intentionally *not* broadcast-joined here: it
would need to be looked up per ``product_id`` which is far higher
cardinality and changes more often (inventory deltas), so it stays a
downstream concern for batch jobs (``spark/jobs/*``) rather than the
streaming enrichment path.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.streaming.config import StreamingConfig

TENANT_METADATA_COLUMNS = ("tenant_id", "tenant_name", "plan", "region", "is_active")


def load_tenant_metadata(spark: SparkSession, config: StreamingConfig) -> DataFrame:
    """Read a static snapshot of ``tenant_config`` for broadcast enrichment."""
    return (
        spark.read.format("jdbc")
        .option("url", config.jdbc_url)
        .option("dbtable", "tenant_config")
        .option("user", config.jdbc_user)
        .option("password", config.jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .select(*TENANT_METADATA_COLUMNS)
    )


def enrich_with_tenant_metadata(events_df: DataFrame, tenant_metadata: DataFrame) -> DataFrame:
    """Left-join events to broadcast tenant metadata, keyed on tenant_id.

    A left join (not inner) is intentional: an event for a tenant that
    isn't in ``tenant_config`` (e.g. a bad/typo'd tenant_id, or the
    metadata snapshot is briefly stale) must still flow through the
    pipeline and be counted — it is not silently dropped for lack of a
    dimension match. ``tenant_metadata_missing`` marks that case so it is
    observable.
    """
    broadcasted = F.broadcast(
        tenant_metadata.select(
            F.col("tenant_id"),
            F.col("tenant_name"),
            F.col("plan").alias("tenant_plan"),
            F.col("region").alias("tenant_region"),
            F.col("is_active").alias("tenant_is_active"),
        )
    )
    enriched = events_df.join(broadcasted, on="tenant_id", how="left")
    enriched = enriched.withColumn("tenant_metadata_missing", F.col("tenant_name").isNull())
    return enriched
