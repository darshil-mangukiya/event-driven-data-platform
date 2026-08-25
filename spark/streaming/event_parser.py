"""Parse raw Kafka records into typed envelope columns.

Kafka gives us opaque ``key``/``value`` byte columns. This module turns
``value`` into the envelope fields defined in ``schemas.py`` while being
explicit about every way a record can fail to parse:

* not valid JSON at all
* valid JSON but missing/null required envelope fields
* an unparseable ``event_timestamp``

None of those failure modes raise an exception or get silently dropped —
they come out as columns (``is_malformed_json``, ``missing_fields``,
``has_invalid_timestamp``) that ``validation.py`` turns into a routing
decision (accept / DLQ).

The envelope's ``payload`` is deliberately kept as a raw JSON string
(``payload_json``) rather than forced into a single typed schema: payload
shape differs per event domain (order vs. payment vs. product, ...), and
using ``get_json_object`` avoids the well-known Spark pitfall where a
``MapType(StringType, StringType)`` target silently nulls out payloads that
mix numeric/boolean/string values.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark.streaming.schemas import REQUIRED_ENVELOPE_FIELDS

# Scalar envelope fields only (everything except `payload`, which is parsed
# separately as raw JSON text — see module docstring).
_ENVELOPE_SCALAR_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("tenant_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_timestamp", T.StringType()),
        T.StructField("source_service", T.StringType()),
        T.StructField("payload_version", T.IntegerType()),
        T.StructField("trace_id", T.StringType()),
        T.StructField("correlation_id", T.StringType()),
        T.StructField("causation_id", T.StringType()),
        T.StructField("idempotency_key", T.StringType()),
        T.StructField("schema_ref", T.StringType()),
    ]
)


def parse_kafka_batch(raw_df: DataFrame) -> DataFrame:
    """Turn a raw Kafka streaming/batch DataFrame into parsed event columns.

    ``raw_df`` must have the standard Kafka source columns
    (key, value, topic, partition, offset, timestamp). Works identically on
    a streaming DataFrame or a static one built for tests.
    """
    decoded = raw_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ingest_timestamp"),
    )

    envelope = F.from_json(
        F.col("raw_value"), _ENVELOPE_SCALAR_SCHEMA, options={"mode": "PERMISSIVE"}
    )

    parsed = decoded.select(
        "*",
        envelope.alias("env"),
        F.get_json_object(F.col("raw_value"), "$.payload").alias("payload_json"),
    )

    parsed = parsed.select(
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_ingest_timestamp",
        "raw_value",
        "payload_json",
        F.col("env.event_id").alias("event_id"),
        F.col("env.tenant_id").alias("tenant_id"),
        F.col("env.event_type").alias("event_type"),
        F.col("env.event_timestamp").alias("event_timestamp_raw"),
        F.to_timestamp(F.col("env.event_timestamp")).alias("event_timestamp"),
        F.col("env.source_service").alias("source_service"),
        F.col("env.payload_version").alias("payload_version"),
        F.col("env.trace_id").alias("trace_id"),
        F.col("env.correlation_id").alias("correlation_id"),
        F.col("env.causation_id").alias("causation_id"),
        F.col("env.idempotency_key").alias("idempotency_key"),
        F.col("env.schema_ref").alias("schema_ref"),
        # get_json_object("$") returns null for a string that isn't valid
        # JSON at all; from_json's PERMISSIVE mode, in contrast, silently
        # returns a struct of all-null fields for the same input, which
        # would be indistinguishable from "valid JSON that happens to be
        # missing every field" if used alone.
        F.get_json_object(F.col("raw_value"), "$").isNull().alias("is_malformed_json"),
    )

    parsed = parsed.withColumn(
        "has_invalid_timestamp",
        (~F.col("is_malformed_json"))
        & F.col("event_timestamp_raw").isNotNull()
        & F.col("event_timestamp").isNull(),
    )
    parsed = parsed.withColumn("missing_fields", _missing_required_fields())
    parsed = parsed.withColumn(
        "is_structurally_valid",
        (~F.col("is_malformed_json"))
        & (~F.col("has_invalid_timestamp"))
        & (F.size(F.col("missing_fields")) == 0),
    )
    return parsed


def _missing_required_fields() -> Column:
    """Array of required envelope field names that are null/missing."""
    checks = [
        F.when(F.col(field_name).isNull(), F.lit(field_name)) for field_name in REQUIRED_ENVELOPE_FIELDS
    ]
    return F.array_except(F.array(*checks), F.array(F.lit(None).cast("string")))
