from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdbc-url", default=os.getenv("SPARK_JDBC_URL", "jdbc:postgresql://postgres:5432/data_platform"))
    parser.add_argument("--jdbc-user", default=os.getenv("POSTGRES_USER", "platform"))
    parser.add_argument("--jdbc-password", default=os.getenv("POSTGRES_PASSWORD", "platform"))
    parser.add_argument("--output", default=os.getenv("SPARK_NORMALIZED_OUTPUT", "/tmp/spark/normalized-events"))
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("event-normalization").getOrCreate()
    raw = (
        spark.read.format("jdbc")
        .option("url", args.jdbc_url)
        .option("user", args.jdbc_user)
        .option("password", args.jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .option(
            "query",
            f"""
            select event_id, tenant_id, event_type, event_timestamp, source_service,
                   payload_version, payload::text as payload, trace_id
            from raw_events
            where event_timestamp >= now() - interval '{args.days} day'
            """,
        )
        .load()
    )

    normalized = (
        raw.withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_domain", F.split("event_type", "\\.").getItem(0))
        .withColumn("normalized_at", F.current_timestamp())
        .dropDuplicates(["event_id"])
    )

    (
        normalized.write.mode("overwrite")
        .partitionBy("tenant_id", "event_date", "event_domain")
        .parquet(args.output)
    )
    spark.stop()


if __name__ == "__main__":
    main()

