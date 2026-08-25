from __future__ import annotations

from spark.streaming.enrichment import enrich_with_tenant_metadata
from spark.streaming.event_parser import parse_kafka_batch
from tests.streaming.conftest import kafka_batch_df, make_envelope


def _tenant_metadata(spark):
    return spark.createDataFrame(
        [("tenant_demo", "Demo Tenant", "growth", "us", True)],
        schema="tenant_id string, tenant_name string, plan string, region string, is_active boolean",
    )


def test_known_tenant_is_enriched(spark):
    df = parse_kafka_batch(kafka_batch_df(spark, [make_envelope(event_id="e1", tenant_id="tenant_demo")]))
    enriched = enrich_with_tenant_metadata(df, _tenant_metadata(spark)).collect()
    assert enriched[0].tenant_name == "Demo Tenant"
    assert enriched[0].tenant_metadata_missing is False


def test_unknown_tenant_is_not_dropped_but_flagged(spark):
    df = parse_kafka_batch(kafka_batch_df(spark, [make_envelope(event_id="e2", tenant_id="tenant_unknown")]))
    enriched = enrich_with_tenant_metadata(df, _tenant_metadata(spark)).collect()
    assert len(enriched) == 1  # left join: event survives even without a metadata match
    assert enriched[0].tenant_metadata_missing is True
    assert enriched[0].tenant_name is None
