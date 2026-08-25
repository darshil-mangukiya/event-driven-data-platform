from __future__ import annotations

import json

from spark.streaming.dlq import build_dlq_payload
from spark.streaming.event_parser import parse_kafka_batch
from spark.streaming.validation import split_valid_invalid, validate_events
from tests.streaming.conftest import kafka_batch_df, make_envelope


def test_dlq_payload_carries_reason_and_original_topic(spark):
    bad = make_envelope(event_id="bad-1", event_type="not.a.real.type")
    df = kafka_batch_df(spark, [bad], topic="platform.events.orders")
    validated = validate_events(parse_kafka_batch(df))
    _valid, invalid = split_valid_invalid(validated)

    dlq_df = build_dlq_payload(invalid).collect()
    assert len(dlq_df) == 1
    row = dlq_df[0]
    assert row.key == "tenant_demo"
    payload = json.loads(row.value)
    assert payload["original_event_id"] == "bad-1"
    assert payload["rejection_reason"] == "unknown_event_type"
    assert payload["original_topic"] == "platform.events.orders"
    assert payload["raw_value"]  # original bytes preserved for replay/debugging
