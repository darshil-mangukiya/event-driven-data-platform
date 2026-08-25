from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("tenant_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_timestamp", T.TimestampType()),
        T.StructField("source_service", T.StringType()),
        T.StructField("payload_version", T.IntegerType()),
        T.StructField("payload", T.MapType(T.StringType(), T.StringType())),
        T.StructField("trace_id", T.StringType()),
    ]
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--checkpoint", default=os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark/checkpoints/streaming-enrichment"))
    parser.add_argument("--output", default=os.getenv("SPARK_ENRICHED_OUTPUT", "/tmp/spark/enriched-events"))
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("streaming-event-enrichment")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option(
            "subscribe",
            "platform.events.orders,platform.events.payments,platform.events.users,platform.events.products,platform.events.system",
        )
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.date_trunc("hour", "event_timestamp"))
        .withColumn("event_domain", F.split("event_type", "\\.").getItem(0))
        .withColumn("processing_time", F.current_timestamp())
        .withWatermark("event_timestamp", "10 minutes")
    )

    query = (
        events.writeStream.format("parquet")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
        .partitionBy("tenant_id", "event_date", "event_domain")
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

