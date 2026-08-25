from __future__ import annotations

import json
import sys
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

pyspark = pytest.importorskip("pyspark", reason="pyspark is required for spark/streaming tests")

import pyspark.sql.types as T  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


def _java_available() -> bool:
    import shutil
    import subprocess

    java = shutil.which("java")
    if not java:
        return False
    try:
        subprocess.run([java, "-version"], capture_output=True, timeout=10, check=True)
        return True
    except Exception:
        return False


if not _java_available():
    pytest.skip(
        "Java runtime not available — Spark Structured Streaming tests are skipped. "
        "Install a JDK (e.g. `brew install openjdk@17`) to run this suite.",
        allow_module_level=True,
    )


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("cloudscale-streaming-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


KAFKA_RAW_SCHEMA = T.StructType(
    [
        T.StructField("key", T.BinaryType()),
        T.StructField("value", T.BinaryType()),
        T.StructField("topic", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("timestamp", T.TimestampType()),
    ]
)


def make_envelope(
    *,
    event_id: str | None = None,
    tenant_id: str = "tenant_demo",
    event_type: str = "order.created",
    event_timestamp: str | None = None,
    source_service: str = "ingestion-service",
    payload_version: int | None = 1,
    payload: dict[str, Any] | None = None,
    trace_id: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    event_id = event_id or str(uuid.uuid4())
    return {
        "event_id": event_id,
        "tenant_id": tenant_id,
        "event_type": event_type,
        "event_timestamp": event_timestamp or datetime.now(timezone.utc).isoformat(),
        "source_service": source_service,
        "payload_version": payload_version,
        "payload": payload
        if payload is not None
        else {
            "order_id": f"order_{event_id}",
            "customer_id": "cust_1",
            "product_id": "prod_1",
            "quantity": 2,
            "unit_price": 19.99,
        },
        "trace_id": trace_id or str(uuid.uuid4()),
        "correlation_id": correlation_id or trace_id or str(uuid.uuid4()),
        "causation_id": causation_id,
        "idempotency_key": idempotency_key or event_id,
        "schema_ref": None,
    }


def kafka_batch_df(spark: SparkSession, records: Iterable[dict[str, Any] | str], topic: str = "platform.events.orders"):
    """Build a static DataFrame shaped like the Kafka source's output columns."""
    rows = []
    for idx, record in enumerate(records):
        raw = record if isinstance(record, str) else json.dumps(record)
        rows.append(
            (
                None,
                raw.encode("utf-8"),
                topic,
                0,
                idx,
                datetime.now(timezone.utc),
            )
        )
    return spark.createDataFrame(rows, schema=KAFKA_RAW_SCHEMA)
