from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

ORDER_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DoubleType()),
        T.StructField("discount_amount", T.DoubleType()),
        T.StructField("marketing_campaign_id", T.StringType()),
    ]
)


def spark() -> SparkSession:
    return (
        SparkSession.builder.appName("tenant-daily-revenue-aggregates")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def jdbc_options(args: argparse.Namespace) -> dict[str, str]:
    return {
        "url": args.jdbc_url,
        "user": args.jdbc_user,
        "password": args.jdbc_password,
        "driver": "org.postgresql.Driver",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdbc-url", default=os.getenv("SPARK_JDBC_URL", "jdbc:postgresql://postgres:5432/data_platform"))
    parser.add_argument("--jdbc-user", default=os.getenv("POSTGRES_USER", "platform"))
    parser.add_argument("--jdbc-password", default=os.getenv("POSTGRES_PASSWORD", "platform"))
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    session = spark()
    raw_events = (
        session.read.format("jdbc")
        .options(**jdbc_options(args))
        .option(
            "query",
            f"""
            select event_id, tenant_id, event_type, event_timestamp, payload::text as payload
            from raw_events
            where event_timestamp >= now() - interval '{args.days} day'
              and event_type in ('order.created','order.updated')
            """,
        )
        .load()
    )

    orders = (
        raw_events.withColumn("payload_json", F.from_json("payload", ORDER_PAYLOAD_SCHEMA))
        .select(
            "tenant_id",
            F.to_date("event_timestamp").alias("metric_date"),
            F.col("payload_json.quantity").alias("quantity"),
            F.col("payload_json.unit_price").alias("unit_price"),
            F.coalesce(F.col("payload_json.discount_amount"), F.lit(0.0)).alias("discount_amount"),
            F.col("payload_json.marketing_campaign_id").alias("marketing_campaign_id"),
        )
        .withColumn("gross_revenue", F.col("quantity") * F.col("unit_price"))
        .withColumn("net_revenue", F.greatest(F.col("gross_revenue") - F.col("discount_amount"), F.lit(0.0)))
    )

    daily = (
        orders.groupBy("tenant_id", "metric_date")
        .agg(
            F.round(F.sum("gross_revenue"), 2).alias("gross_revenue"),
            F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
            F.count("*").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum(F.when(F.col("marketing_campaign_id").isNotNull(), F.lit(3.50)).otherwise(F.lit(0.0))).alias("marketing_spend"),
            F.round(
                F.sum(F.when(F.col("marketing_campaign_id").isNotNull(), F.col("net_revenue")).otherwise(F.lit(0.0))),
                2,
            ).alias("marketing_attributed_revenue"),
            F.count("*").alias("events_processed"),
        )
        .withColumn("new_users", F.lit(0))
        .withColumn("active_users", F.lit(0))
        .withColumn("churn_signal_count", F.lit(0))
        .withColumn("payment_success_count", F.lit(0))
        .withColumn("payment_failure_count", F.lit(0))
        .withColumn("updated_at", F.current_timestamp())
    )

    (
        daily.write.format("jdbc")
        .options(**jdbc_options(args))
        .option("dbtable", "tenant_metrics_daily_spark_stage")
        .mode("overwrite")
        .save()
    )
    session.stop()


if __name__ == "__main__":
    main()

