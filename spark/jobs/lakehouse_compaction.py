from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default=os.getenv("LAKEHOUSE_BRONZE_EVENTS", "s3a://data-platform/bronze/events"),
    )
    parser.add_argument(
        "--output",
        default=os.getenv("LAKEHOUSE_SILVER_EVENTS", "s3a://data-platform/silver/events"),
    )
    parser.add_argument("--target-file-mb", type=int, default=128)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("lakehouse-event-compaction")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    events = spark.read.parquet(args.input)
    compacted = (
        events.dropDuplicates(["event_id"])
        .withColumn("event_date", F.to_date("event_timestamp"))
        .repartition("tenant_id", "event_date", "event_domain")
    )

    (
        compacted.write.mode("overwrite")
        .partitionBy("tenant_id", "event_date", "event_domain")
        .parquet(args.output)
    )
    spark.stop()


if __name__ == "__main__":
    main()
