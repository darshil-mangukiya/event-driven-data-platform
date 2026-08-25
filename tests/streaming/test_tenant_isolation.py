"""Tenant isolation through the streaming transformation pipeline.

Proves Tenant A's events never contribute to Tenant B's windowed
aggregates, and that an identical event_id reused by two different tenants
is treated as two distinct events (not deduplicated against each other) —
required because the dedup key is (tenant_id, event_id), not event_id
alone.
"""

from __future__ import annotations

from datetime import datetime, timezone

from spark.streaming.aggregations import build_window_aggregates
from spark.streaming.config import StreamingConfig
from spark.streaming.deduplication import mark_duplicates_within_batch
from spark.streaming.enrichment import enrich_with_tenant_metadata
from spark.streaming.event_parser import parse_kafka_batch
from spark.streaming.validation import split_valid_invalid, validate_events
from spark.streaming.watermarking import classify_lateness, split_by_lateness
from tests.streaming.conftest import kafka_batch_df, make_envelope

CONFIG = StreamingConfig(window_duration="5 minutes")


def _tenant_metadata(spark):
    return spark.createDataFrame(
        [("tenant_a", "Tenant A", "growth", "us", True), ("tenant_b", "Tenant B", "growth", "us", True)],
        schema="tenant_id string, tenant_name string, plan string, region string, is_active boolean",
    )


def test_tenant_a_revenue_never_appears_under_tenant_b(spark):
    now = datetime.now(timezone.utc).isoformat()
    records = [
        make_envelope(
            event_id="a1",
            tenant_id="tenant_a",
            event_timestamp=now,
            payload={"order_id": "a1", "customer_id": "c1", "product_id": "p1", "quantity": 1, "unit_price": 100.0},
        ),
        make_envelope(
            event_id="b1",
            tenant_id="tenant_b",
            event_timestamp=now,
            payload={"order_id": "b1", "customer_id": "c2", "product_id": "p2", "quantity": 1, "unit_price": 5.0},
        ),
    ]
    df = kafka_batch_df(spark, records)
    validated = validate_events(parse_kafka_batch(df))
    valid, _invalid = split_valid_invalid(validated)
    classified = classify_lateness(valid, CONFIG)
    aggregatable, _rejected = split_by_lateness(classified)
    enriched = enrich_with_tenant_metadata(aggregatable, _tenant_metadata(spark))
    aggregated = build_window_aggregates(enriched, CONFIG).collect()

    by_tenant = {row.tenant_id: row.revenue for row in aggregated}
    assert by_tenant["tenant_a"] == 100.0
    assert by_tenant["tenant_b"] == 5.0
    # No cross-tenant leakage: exactly one row per tenant for this window/domain.
    assert len([r for r in aggregated if r.tenant_id == "tenant_a"]) == 1
    assert len([r for r in aggregated if r.tenant_id == "tenant_b"]) == 1


def test_same_event_id_across_tenants_is_not_treated_as_duplicate(spark):
    shared_event_id = "evt-shared-id"
    records = [
        make_envelope(event_id=shared_event_id, tenant_id="tenant_a"),
        make_envelope(event_id=shared_event_id, tenant_id="tenant_b"),
    ]
    df = kafka_batch_df(spark, records)
    parsed = parse_kafka_batch(df)
    marked = mark_duplicates_within_batch(parsed).collect()
    assert all(row.is_duplicate_in_batch is False for row in marked)


def test_true_duplicate_within_same_tenant_is_flagged(spark):
    shared_event_id = "evt-dup"
    records = [
        make_envelope(event_id=shared_event_id, tenant_id="tenant_a"),
        make_envelope(event_id=shared_event_id, tenant_id="tenant_a"),
    ]
    df = kafka_batch_df(spark, records)
    parsed = parse_kafka_batch(df)
    marked = mark_duplicates_within_batch(parsed).collect()
    duplicate_flags = sorted(row.is_duplicate_in_batch for row in marked)
    assert duplicate_flags == [False, True]
