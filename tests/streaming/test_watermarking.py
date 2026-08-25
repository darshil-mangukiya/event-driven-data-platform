from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spark.streaming.config import StreamingConfig
from spark.streaming.event_parser import parse_kafka_batch
from spark.streaming.validation import split_valid_invalid, validate_events
from spark.streaming.watermarking import (
    LATE_ACCEPTED,
    LATE_REJECTED,
    ON_TIME,
    classify_lateness,
    split_by_lateness,
)
from tests.streaming.conftest import kafka_batch_df, make_envelope

CONFIG = StreamingConfig(late_accept_threshold_seconds=60, late_reject_threshold_seconds=600, watermark_delay="10 minutes")


def _prepared(spark, records):
    df = kafka_batch_df(spark, records)
    validated = validate_events(parse_kafka_batch(df))
    valid, _invalid = split_valid_invalid(validated)
    return valid


def test_on_time_event_classified_on_time(spark):
    now_iso = datetime.now(timezone.utc).isoformat()
    df = _prepared(spark, [make_envelope(event_id="e1", event_timestamp=now_iso)])
    classified = classify_lateness(df, CONFIG).collect()
    assert classified[0].lateness_classification == ON_TIME


def test_moderately_late_event_is_late_accepted(spark):
    ts = (datetime.now(timezone.utc) - timedelta(seconds=300)).isoformat()
    df = _prepared(spark, [make_envelope(event_id="e2", event_timestamp=ts)])
    classified = classify_lateness(df, CONFIG).collect()
    assert classified[0].lateness_classification == LATE_ACCEPTED
    assert classified[0].lateness_seconds >= 290


def test_very_late_event_is_late_rejected(spark):
    ts = (datetime.now(timezone.utc) - timedelta(seconds=3600)).isoformat()
    df = _prepared(spark, [make_envelope(event_id="e3", event_timestamp=ts)])
    classified = classify_lateness(df, CONFIG).collect()
    assert classified[0].lateness_classification == LATE_REJECTED


def test_split_by_lateness_excludes_only_rejected(spark):
    now_iso = datetime.now(timezone.utc).isoformat()
    late_ts = (datetime.now(timezone.utc) - timedelta(seconds=300)).isoformat()
    rejected_ts = (datetime.now(timezone.utc) - timedelta(seconds=3600)).isoformat()
    records = [
        make_envelope(event_id="on-time", event_timestamp=now_iso),
        make_envelope(event_id="late-accepted", event_timestamp=late_ts),
        make_envelope(event_id="late-rejected", event_timestamp=rejected_ts),
    ]
    df = _prepared(spark, records)
    classified = classify_lateness(df, CONFIG)
    aggregatable, rejected = split_by_lateness(classified)

    aggregatable_ids = {row.event_id for row in aggregatable.collect()}
    rejected_ids = {row.event_id for row in rejected.collect()}

    assert aggregatable_ids == {"on-time", "late-accepted"}
    assert rejected_ids == {"late-rejected"}
