"""Run real events through the real spark/streaming pipeline functions,
locally, without needing a live Kafka broker.

Several reliability scenarios (poison-event, duplicate-event, late-event)
need to prove "the platform's actual validation/dedup/watermark code
classifies this input as X" — that's a genuine, executable claim as long as
it runs the real `spark.streaming.*` functions, which this module makes
possible with a local, ephemeral SparkSession (``local[2]``) and a
Kafka-shaped batch DataFrame built directly from the input records — the
same shape `spark/streaming/kafka_source.py` produces from a real
``readStream``. This is deliberately separate from ``tests/streaming/conftest.py``
(pytest-only fixtures) since this is a runtime tool, not test scaffolding.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import datetime, timezone
from typing import Any

_spark_session: Any | None = None


def spark_available() -> bool:
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False
    java = shutil.which("java")
    if not java:
        return False
    try:
        subprocess.run([java, "-version"], capture_output=True, timeout=10, check=True)
        return True
    except Exception:
        return False


def get_or_create_spark():
    global _spark_session
    if _spark_session is None:
        from pyspark.sql import SparkSession

        _spark_session = (
            SparkSession.builder.appName("cloudscale-reliability-harness")
            .master("local[2]")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        _spark_session.sparkContext.setLogLevel("WARN")
    return _spark_session


def kafka_shaped_batch(spark, records: list[dict[str, Any] | str], topic: str = "platform.events.orders"):
    import pyspark.sql.types as T

    schema = T.StructType(
        [
            T.StructField("key", T.BinaryType()),
            T.StructField("value", T.BinaryType()),
            T.StructField("topic", T.StringType()),
            T.StructField("partition", T.IntegerType()),
            T.StructField("offset", T.LongType()),
            T.StructField("timestamp", T.TimestampType()),
        ]
    )
    rows = []
    for idx, record in enumerate(records):
        raw = record if isinstance(record, str) else json.dumps(record)
        rows.append((None, raw.encode("utf-8"), topic, 0, idx, datetime.now(timezone.utc)))
    return spark.createDataFrame(rows, schema=schema)
