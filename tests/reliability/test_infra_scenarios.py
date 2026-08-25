"""Slower reliability-scenario tests: real network timeouts against
deliberately-unreachable hosts (redis-outage, db-outage), and real Spark
streaming-query start/stop cycles (consumer-interruption). Marked
"integration" — run with `make reliability-test`, not the default fast
suite.
"""

from __future__ import annotations

import shutil
import subprocess

import pytest

from reliability.scenarios import consumer_interruption, db_outage, redis_outage


def _java_available() -> bool:
    java = shutil.which("java")
    if not java:
        return False
    try:
        subprocess.run([java, "-version"], capture_output=True, timeout=10, check=True)
        return True
    except Exception:
        return False


@pytest.mark.integration
def test_redis_outage_scenario_falls_back_without_raising():
    result = redis_outage.run()
    step = next(s for s in result.steps if s.name == "exercise_redis_cache_against_unreachable_host")
    assert step.status == "verified", step.detail
    assert step.evidence["get_json_result"] is None
    assert step.evidence["unavailable_count"] == 3


@pytest.mark.integration
def test_db_outage_scenario_raises_sink_error_after_bounded_retries():
    result = db_outage.run()
    step = next(s for s in result.steps if s.name == "write_fails_with_bounded_retries")
    assert step.status == "verified", step.detail
    log_step = next(s for s in result.steps if s.name == "log_failure_never_raises")
    assert log_step.status == "verified"


@pytest.mark.integration
@pytest.mark.skipif(not _java_available(), reason="Java runtime required")
def test_consumer_interruption_scenario_recovers_from_checkpoint():
    result = consumer_interruption.run()
    step = next(s for s in result.steps if s.name == "restart_from_same_checkpoint_recovers_without_exceeding_key_space")
    assert step.status == "verified", step.detail
