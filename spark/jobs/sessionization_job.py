from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

USER_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("user_id", T.StringType()),
        T.StructField("action", T.StringType()),
        T.StructField("session_id", T.StringType()),
        T.StructField("page", T.StringType()),
        T.StructField("duration_seconds", T.IntegerType()),
        T.StructField("marketing_campaign_id", T.StringType()),
    ]
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

    spark = SparkSession.builder.appName("tenant-user-sessionization").getOrCreate()
    raw_users = (
        spark.read.format("jdbc")
        .options(**jdbc_options(args))
        .option(
            "query",
            f"""
            select event_id, tenant_id, event_timestamp, payload::text as payload
            from raw_events
            where event_type in ('user.activity','user.signed_up','user.churn_signal')
              and event_timestamp >= now() - interval '{args.days} day'
            """,
        )
        .load()
    )

    events = (
        raw_users.withColumn("payload_json", F.from_json("payload", USER_PAYLOAD_SCHEMA))
        .select(
            "tenant_id",
            "event_id",
            "event_timestamp",
            F.col("payload_json.user_id").alias("user_id"),
            F.coalesce(F.col("payload_json.session_id"), F.concat(F.lit("sessionless:"), F.col("payload_json.user_id"))).alias("session_id"),
            F.col("payload_json.action").alias("action"),
            F.col("payload_json.page").alias("page"),
            F.coalesce(F.col("payload_json.duration_seconds"), F.lit(0)).alias("duration_seconds"),
        )
    )

    window = Window.partitionBy("tenant_id", "user_id", "session_id").orderBy("event_timestamp")
    sessionized = (
        events.withColumn("previous_event_timestamp", F.lag("event_timestamp").over(window))
        .withColumn(
            "minutes_since_previous_event",
            (F.col("event_timestamp").cast("long") - F.col("previous_event_timestamp").cast("long")) / 60,
        )
        .groupBy("tenant_id", "user_id", "session_id")
        .agg(
            F.min("event_timestamp").alias("session_start_at"),
            F.max("event_timestamp").alias("session_end_at"),
            F.count("*").alias("event_count"),
            F.sum("duration_seconds").alias("engaged_seconds"),
            F.collect_set("page").alias("pages_seen"),
        )
        .withColumn("sessionized_at", F.current_timestamp())
    )

    (
        sessionized.write.format("jdbc")
        .options(**jdbc_options(args))
        .option("dbtable", "tenant_user_session_summary_stage")
        .mode("overwrite")
        .save()
    )
    spark.stop()


if __name__ == "__main__":
    main()

