from __future__ import annotations

from spark.streaming.event_parser import parse_kafka_batch
from tests.streaming.conftest import kafka_batch_df, make_envelope


def test_valid_envelope_parses_structurally_valid(spark):
    df = kafka_batch_df(spark, [make_envelope(event_id="evt-1")])
    parsed = parse_kafka_batch(df).collect()
    assert len(parsed) == 1
    row = parsed[0]
    assert row.is_malformed_json is False
    assert row.has_invalid_timestamp is False
    assert row.missing_fields == []
    assert row.is_structurally_valid is True
    assert row.event_id == "evt-1"
    assert row.tenant_id == "tenant_demo"


def test_malformed_json_is_flagged_not_dropped(spark):
    df = kafka_batch_df(spark, ["{not valid json at all"])
    parsed = parse_kafka_batch(df).collect()
    assert len(parsed) == 1
    row = parsed[0]
    assert row.is_malformed_json is True
    assert row.is_structurally_valid is False
    assert row.raw_value == "{not valid json at all"


def test_missing_required_field_is_flagged(spark):
    record = make_envelope(event_id="evt-2")
    del record["source_service"]
    df = kafka_batch_df(spark, [record])
    parsed = parse_kafka_batch(df).collect()
    row = parsed[0]
    assert row.is_malformed_json is False
    assert "source_service" in row.missing_fields
    assert row.is_structurally_valid is False


def test_invalid_timestamp_is_flagged(spark):
    record = make_envelope(event_id="evt-3", event_timestamp="not-a-timestamp")
    df = kafka_batch_df(spark, [record])
    parsed = parse_kafka_batch(df).collect()
    row = parsed[0]
    assert row.has_invalid_timestamp is True
    assert row.event_timestamp is None
    assert row.is_structurally_valid is False


def test_payload_json_retains_nested_object(spark):
    record = make_envelope(
        event_id="evt-4",
        payload={"order_id": "o1", "customer_id": "c1", "product_id": "p1", "quantity": 3, "unit_price": 9.5},
    )
    df = kafka_batch_df(spark, [record])
    parsed = parse_kafka_batch(df).collect()
    row = parsed[0]
    import json

    payload = json.loads(row.payload_json)
    assert payload["quantity"] == 3
    assert payload["unit_price"] == 9.5


def test_null_kafka_value_is_flagged_malformed(spark):
    from datetime import datetime, timezone

    from tests.streaming.conftest import KAFKA_RAW_SCHEMA

    df = spark.createDataFrame(
        [(None, None, "platform.events.orders", 0, 0, datetime.now(timezone.utc))],
        schema=KAFKA_RAW_SCHEMA,
    )
    parsed = parse_kafka_batch(df).collect()
    assert parsed[0].is_malformed_json is True
