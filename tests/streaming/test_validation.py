from __future__ import annotations

from spark.streaming.event_parser import parse_kafka_batch
from spark.streaming.validation import (
    REASON_MISSING_PAYLOAD_FIELDS,
    REASON_UNKNOWN_EVENT_TYPE,
    REASON_UNSUPPORTED_VERSION,
    VALID,
    split_valid_invalid,
    validate_events,
)
from tests.streaming.conftest import kafka_batch_df, make_envelope


def _validated(spark, records):
    df = kafka_batch_df(spark, records)
    return validate_events(parse_kafka_batch(df))


def test_valid_order_event_passes(spark):
    rows = _validated(spark, [make_envelope(event_id="e1")]).collect()
    assert rows[0].validation_status == VALID
    assert rows[0].event_domain == "orders"


def test_unknown_event_type_is_invalid(spark):
    rows = _validated(spark, [make_envelope(event_id="e2", event_type="order.deleted")]).collect()
    assert rows[0].validation_status == "invalid"
    assert rows[0].validation_reason == REASON_UNKNOWN_EVENT_TYPE


def test_unsupported_payload_version_is_invalid(spark):
    rows = _validated(spark, [make_envelope(event_id="e3", payload_version=99)]).collect()
    assert rows[0].validation_status == "invalid"
    assert rows[0].validation_reason == REASON_UNSUPPORTED_VERSION


def test_missing_payload_field_is_invalid(spark):
    record = make_envelope(event_id="e4", payload={"order_id": "o1", "customer_id": "c1"})
    rows = _validated(spark, [record]).collect()
    assert rows[0].validation_status == "invalid"
    assert rows[0].validation_reason == REASON_MISSING_PAYLOAD_FIELDS


def test_payment_event_requires_payment_fields(spark):
    record = make_envelope(
        event_id="e5",
        event_type="payment.captured",
        payload={"payment_id": "p1", "order_id": "o1", "customer_id": "c1", "amount": 20.0, "status": "captured"},
    )
    rows = _validated(spark, [record]).collect()
    assert rows[0].validation_status == VALID
    assert rows[0].event_domain == "payments"


def test_split_valid_invalid_partitions_correctly(spark):
    good = make_envelope(event_id="good-1")
    bad = make_envelope(event_id="bad-1", event_type="unknown.type")
    validated = _validated(spark, [good, bad])
    valid_df, invalid_df = split_valid_invalid(validated)
    assert valid_df.count() == 1
    assert invalid_df.count() == 1
    assert valid_df.collect()[0].event_id == "good-1"
    assert invalid_df.collect()[0].event_id == "bad-1"
