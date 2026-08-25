from __future__ import annotations

from spark.streaming.config import StreamingConfig, interval_to_seconds, validate_config
from spark.streaming.deduplication import DEDUP_KEYS


def test_default_config_is_valid():
    config = StreamingConfig()
    assert validate_config(config) == []


def test_interval_to_seconds():
    assert interval_to_seconds("10 minutes") == 600
    assert interval_to_seconds("30 seconds") == 30
    assert interval_to_seconds("1 hour") == 3600


def test_late_reject_threshold_cannot_exceed_watermark():
    config = StreamingConfig(watermark_delay="1 minute", late_accept_threshold_seconds=10, late_reject_threshold_seconds=120)
    errors = validate_config(config)
    assert any("late_reject_threshold_seconds" in e for e in errors)


def test_late_reject_must_exceed_late_accept():
    config = StreamingConfig(late_accept_threshold_seconds=100, late_reject_threshold_seconds=50)
    errors = validate_config(config)
    assert any("greater than" in e for e in errors)


def test_checkpoint_paths_do_not_collide():
    config = StreamingConfig(checkpoint_root="/tmp/spark/checkpoints/test-run")
    a = config.checkpoint_path("aggregates")
    b = config.checkpoint_path("dlq")
    assert a != b
    assert a.startswith(config.checkpoint_root)
    assert b.startswith(config.checkpoint_root)


def test_dedup_keys_include_tenant_and_event_id():
    # Tenant isolation requirement: dedupe key must be scoped by tenant_id so
    # that identical event_ids across two tenants are never conflated.
    assert DEDUP_KEYS == ("tenant_id", "event_id")


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker-1:9092,broker-2:9092")
    monkeypatch.setenv("STREAMING_SUBSCRIBE_TOPICS", "platform.events.orders,platform.events.payments")
    monkeypatch.setenv("STREAMING_TENANT_FILTER", "tenant_a, tenant_b")
    monkeypatch.setenv("STREAMING_METRICS_PORT", "9111")
    config = StreamingConfig.from_env()
    assert config.kafka_bootstrap_servers == "broker-1:9092,broker-2:9092"
    assert config.subscribe_topics == ("platform.events.orders", "platform.events.payments")
    assert config.tenant_filter == ("tenant_a", "tenant_b")
    assert config.metrics_port == 9111
