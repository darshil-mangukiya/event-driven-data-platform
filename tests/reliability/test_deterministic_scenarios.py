"""Fast reliability-scenario tests: real Spark, real reconciliation logic,
no network/live-infra dependency (each scenario's live-infra step
self-reports "not_run" when unreachable, which these tests also assert).
"""

from __future__ import annotations

import shutil
import subprocess

import pytest

from reliability.scenarios import (
    consumer_lag,
    duplicate_event,
    late_event,
    poison_event,
    reconciliation_mismatch,
)


def _java_available() -> bool:
    java = shutil.which("java")
    if not java:
        return False
    try:
        subprocess.run([java, "-version"], capture_output=True, timeout=10, check=True)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _java_available(), reason="Java runtime required for reliability scenarios that use Spark")


def test_poison_event_scenario_detects_unsupported_version():
    result = poison_event.run()
    step = next(s for s in result.steps if s.name == "run_real_validation_pipeline")
    assert step.status == "verified"
    assert step.evidence["validation_reason"] == "unsupported_payload_version"
    assert result.overall_status in {"passed", "not_run"}


def test_duplicate_event_scenario_detects_same_batch_duplicate():
    result = duplicate_event.run()
    step = next(s for s in result.steps if s.name == "detect_same_batch_duplicate")
    assert step.status == "verified"
    assert sorted(step.evidence["is_duplicate_in_batch_values"]) == [False, True]


def test_late_event_scenario_classifies_correctly():
    result = late_event.run()
    step = next(s for s in result.steps if s.name == "classify_and_split_late_events")
    assert step.status == "verified"
    assert step.evidence["aggregatable_event_ids"] == ["reliability-late-accepted"]
    assert step.evidence["rejected_event_ids"] == ["reliability-late-rejected"]


def test_reconciliation_mismatch_scenario_flags_the_mismatch():
    result = reconciliation_mismatch.run()
    step = next(s for s in result.steps if s.name == "detect_mismatch_with_real_reconciliation_logic")
    assert step.status == "verified"


def test_consumer_lag_scenario_exceeds_alert_threshold():
    result = consumer_lag.run()
    step = next(s for s in result.steps if s.name == "exercise_lag_gauge_and_alert_threshold")
    assert step.status == "verified"
    assert step.evidence["synthetic_lag_seconds"] > step.evidence["threshold_seconds"]


def test_poison_event_classifies_a_broken_kafka_client_as_not_run_not_failed(monkeypatch):
    """Regression for a real, live-reproduced failure: this environment's
    pinned kafka-python raises `ModuleNotFoundError: No module named
    'kafka.vendor.six.moves'` on Python 3.12 whenever something else is
    reachable on the default Kafka port (found during the final hardening
    pass — an unrelated Docker project on this machine happened to have a
    broker listening on the same well-known port, so `kafka_reachable()`'s
    plain TCP check correctly said "something is there"). A broken/missing
    client *library* is an environment/tooling problem, not evidence the
    platform's own poison-event handling failed — it must not flip
    `overall_status` to "failed" and mask the real, already-verified
    validation/DLQ-routing assertions above it.
    """
    monkeypatch.setattr(poison_event, "kafka_reachable", lambda *_args, **_kwargs: True)

    def _raise_module_not_found(*_args, **_kwargs):
        raise ModuleNotFoundError("No module named 'kafka.vendor.six.moves'")

    monkeypatch.setattr(poison_event, "publish_raw", _raise_module_not_found)

    result = poison_event.run()
    publish_step = next(s for s in result.steps if s.name == "publish_poison_event_to_kafka")
    assert publish_step.status == "not_run"
    assert "kafka.vendor.six.moves" in publish_step.detail
    assert result.overall_status in {"passed", "not_run"}


def test_poison_event_still_fails_on_a_genuine_publish_error(monkeypatch):
    """The fix above must not swallow real publish failures — only
    ImportError/ModuleNotFoundError (a broken client library) is
    downgraded to not_run; any other exception (e.g. a real broker error)
    must still surface as a genuine "failed" step.
    """
    monkeypatch.setattr(poison_event, "kafka_reachable", lambda *_args, **_kwargs: True)

    def _raise_broker_error(*_args, **_kwargs):
        raise RuntimeError("NoBrokersAvailable")

    monkeypatch.setattr(poison_event, "publish_raw", _raise_broker_error)

    result = poison_event.run()
    publish_step = next(s for s in result.steps if s.name == "publish_poison_event_to_kafka")
    assert publish_step.status == "failed"
    assert result.overall_status == "failed"


def test_every_deterministic_scenario_produces_full_incident_fields():
    for module in (poison_event, duplicate_event, late_event, reconciliation_mismatch, consumer_lag):
        result = module.run()
        assert result.expected_behavior
        assert result.detection_method
        assert result.root_cause
        assert result.recovery
        assert result.corrective_action
        assert result.preventive_control
        assert result.steps, f"{module.SCENARIO_ID} produced no steps"
