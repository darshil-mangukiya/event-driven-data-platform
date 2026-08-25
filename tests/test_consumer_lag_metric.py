"""Tests for the corresponding verification Kafka consumer-lag Prometheus metric
(platform_shared.metrics.record_consumer_lag,
processing-service's KafkaProcessingWorker._record_consumer_lag).

This is the same lag signal KEDA's ScaledObject scales on
(evidence/validation/keda-autoscaling-live-verification.md), also exposed via
Prometheus so it's visible without a Kubernetes/KEDA deployment.
"""

from __future__ import annotations

import importlib.util
import sys
from collections import namedtuple
from pathlib import Path
from unittest.mock import MagicMock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "services" / "shared"))

from platform_shared.metrics import KAFKA_CONSUMER_LAG, record_consumer_lag  # noqa: E402

TopicPartition = namedtuple("TopicPartition", ["topic", "partition"])


def test_record_consumer_lag_sets_the_gauge() -> None:
    record_consumer_lag("processing-service", "platform.events.orders", 0, 42)
    value = KAFKA_CONSUMER_LAG.labels(service="processing-service", topic="platform.events.orders", partition="0")._value.get()
    assert value == 42


def _load_worker_module():
    """worker.py imports `from app.processors import ...` / `from app.repository
    import ...` (bare `app.`), which collides with any other test file's
    cached `sys.modules["app"]` for a *different* service — the documented
    cross-service import hazard (see docs/testing-strategy.md "A Recurring
    Cross-Service Import Hazard"). Clearing `app`/`app.*` from sys.modules
    before loading, and inserting this service's own path first, is the
    same fix `tests/test_ops_console.py` uses for the same class of issue.
    """
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    service_path = str(PROJECT_ROOT / "services" / "processing-service")
    if service_path not in sys.path:
        sys.path.insert(0, service_path)

    spec = importlib.util.spec_from_file_location("worker", PROJECT_ROOT / "services" / "processing-service" / "app" / "worker.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_worker_record_consumer_lag_computes_high_water_mark_minus_position() -> None:
    worker_module = _load_worker_module()
    worker = worker_module.KafkaProcessingWorker.__new__(worker_module.KafkaProcessingWorker)

    tp = TopicPartition("platform.events.orders", 0)
    fake_consumer = MagicMock()
    fake_consumer.assignment.return_value = {tp}
    fake_consumer.end_offsets.return_value = {tp: 100}
    fake_consumer.position.return_value = 80
    worker._consumer = fake_consumer

    worker._record_consumer_lag()

    value = KAFKA_CONSUMER_LAG.labels(service="processing-service", topic="platform.events.orders", partition="0")._value.get()
    assert value == 20


def test_worker_record_consumer_lag_never_raises_on_failure() -> None:
    worker_module = _load_worker_module()
    worker = worker_module.KafkaProcessingWorker.__new__(worker_module.KafkaProcessingWorker)

    fake_consumer = MagicMock()
    fake_consumer.assignment.side_effect = RuntimeError("broker unavailable")
    worker._consumer = fake_consumer

    worker._record_consumer_lag()  # must not raise


def test_worker_record_consumer_lag_does_nothing_with_no_assignment() -> None:
    worker_module = _load_worker_module()
    worker = worker_module.KafkaProcessingWorker.__new__(worker_module.KafkaProcessingWorker)

    fake_consumer = MagicMock()
    fake_consumer.assignment.return_value = set()
    worker._consumer = fake_consumer

    worker._record_consumer_lag()
    fake_consumer.end_offsets.assert_not_called()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
