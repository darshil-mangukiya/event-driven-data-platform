"""Kafka source construction for the Structured Streaming job.

Kafka has no server-side "give me only tenant X" filter — the broker
doesn't parse message payloads — so tenant scoping (``STREAMING_TENANT_FILTER``)
is applied as an ordinary DataFrame filter immediately after parsing
(see :func:`spark.streaming.event_parser.parse_kafka_batch` callers in
``streaming_job.py``), not at the source. It exists for local/demo
scenarios where you want to run the pipeline against a single tenant's
events without touching the shared topics.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from spark.streaming.config import StreamingConfig


def build_kafka_read_stream(spark: SparkSession, config: StreamingConfig) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", ",".join(config.subscribe_topics))
        .option("startingOffsets", config.starting_offsets)
        .option("failOnDataLoss", "false")
    )
    if config.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
    return reader.load()
