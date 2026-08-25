from __future__ import annotations

from datetime import datetime, timezone

from spark.streaming.aggregations import build_window_aggregates, to_long_format
from spark.streaming.config import StreamingConfig
from spark.streaming.enrichment import enrich_with_tenant_metadata
from spark.streaming.event_parser import parse_kafka_batch
from spark.streaming.validation import split_valid_invalid, validate_events
from spark.streaming.watermarking import classify_lateness, split_by_lateness
from tests.streaming.conftest import kafka_batch_df, make_envelope

CONFIG = StreamingConfig(window_duration="5 minutes")


def _pipeline_to_aggregatable(spark, records):
    df = kafka_batch_df(spark, records)
    validated = validate_events(parse_kafka_batch(df))
    valid, _invalid = split_valid_invalid(validated)
    classified = classify_lateness(valid, CONFIG)
    aggregatable, _rejected = split_by_lateness(classified)
    return aggregatable


def _tenant_metadata(spark):
    return spark.createDataFrame(
        [("tenant_demo", "Demo Tenant", "growth", "us", True), ("tenant_other", "Other Tenant", "scale", "eu", True)],
        schema="tenant_id string, tenant_name string, plan string, region string, is_active boolean",
    )


def test_revenue_aggregation_sums_order_and_payment_revenue(spark):
    now = datetime.now(timezone.utc).isoformat()
    records = [
        make_envelope(
            event_id="o1",
            event_type="order.created",
            event_timestamp=now,
            payload={"order_id": "o1", "customer_id": "c1", "product_id": "p1", "quantity": 2, "unit_price": 10.0},
        ),
        make_envelope(
            event_id="p1",
            event_type="payment.captured",
            event_timestamp=now,
            payload={"payment_id": "p1", "order_id": "o1", "customer_id": "c1", "amount": 20.0, "status": "captured"},
        ),
    ]
    aggregatable = _pipeline_to_aggregatable(spark, records)
    enriched = enrich_with_tenant_metadata(aggregatable, _tenant_metadata(spark))
    aggregated = build_window_aggregates(enriched, CONFIG).collect()

    orders_row = next(r for r in aggregated if r.event_domain == "orders")
    payments_row = next(r for r in aggregated if r.event_domain == "payments")
    # order.created: unit_price(10.0) * quantity(2) = 20.0
    assert orders_row.revenue == 20.0
    assert orders_row.order_count == 1
    assert orders_row.units_sold == 2
    # payment.captured: amount = 20.0, and it counts toward payment_success_count
    assert payments_row.revenue == 20.0
    assert payments_row.payment_success_count == 1


def test_payment_health_counts_success_and_failure_separately(spark):
    now = datetime.now(timezone.utc).isoformat()
    records = [
        make_envelope(
            event_id="pay-ok",
            event_type="payment.captured",
            event_timestamp=now,
            payload={"payment_id": "pay-ok", "order_id": "o1", "customer_id": "c1", "amount": 50.0, "status": "captured"},
        ),
        make_envelope(
            event_id="pay-fail",
            event_type="payment.failed",
            event_timestamp=now,
            payload={"payment_id": "pay-fail", "order_id": "o2", "customer_id": "c2", "amount": 30.0, "status": "failed"},
        ),
    ]
    aggregatable = _pipeline_to_aggregatable(spark, records)
    enriched = enrich_with_tenant_metadata(aggregatable, _tenant_metadata(spark))
    row = build_window_aggregates(enriched, CONFIG).collect()[0]
    assert row.payment_success_count == 1
    assert row.payment_failure_count == 1
    # payment.failed does not contribute to revenue
    assert row.revenue == 50.0


def test_to_long_format_produces_tidy_metric_rows(spark):
    now = datetime.now(timezone.utc).isoformat()
    records = [make_envelope(event_id="o1", event_timestamp=now)]
    aggregatable = _pipeline_to_aggregatable(spark, records)
    enriched = enrich_with_tenant_metadata(aggregatable, _tenant_metadata(spark))
    aggregated = build_window_aggregates(enriched, CONFIG)
    long_df = to_long_format(aggregated).collect()
    metric_names = {row.metric_name for row in long_df}
    assert {"revenue", "order_count", "units_sold", "payment_success_count", "payment_failure_count", "event_count"} <= metric_names
