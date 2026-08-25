"""Tests for reliability/runner.py — the orchestration layer between one
scenario module's `.run()` and (a) evidence-file writing, (b) best-effort
persistence to pipeline_run_log, (c) the CLI-facing summary dict.

coverage analysis found this file at 41% coverage: the 8 reliability
scenarios themselves are directly unit-tested (tests/reliability/), and the
runner is also exercised through the live CLI,
but the runner's own orchestration logic — unknown scenario_id handling,
run_all_scenarios' failed-scenario aggregation, and
persist_outcome_to_pipeline_run_log's best-effort/never-raises contract —
had no automated regression test of its own.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from reliability.models import ScenarioResult, StepResult
from reliability.runner import (
    persist_outcome_to_pipeline_run_log,
    run_all_scenarios,
    run_scenario,
)
from spark.streaming.config import StreamingConfig


def _fake_result(status: str) -> ScenarioResult:
    step = StepResult(name="check", status="verified" if status != "failed" else "failed", detail="detail")
    result = ScenarioResult(
        scenario_id="fake-scenario",
        title="Fake Scenario",
        component="test",
        expected_behavior="expected",
        detection_method="detected",
        impact="impact",
        root_cause="cause",
        recovery="recovery",
        corrective_action="action",
        preventive_control="control",
        steps=[step],
    )
    result.ended_at = datetime.now(timezone.utc)
    return result


class _FakeScenarioModule:
    def __init__(self, status: str) -> None:
        self._status = status

    def run(self, _config: StreamingConfig) -> ScenarioResult:
        return _fake_result(self._status)


# ---------------------------------------------------------------------------
# run_scenario: unknown scenario_id
# ---------------------------------------------------------------------------


def test_run_scenario_returns_failed_status_for_unknown_scenario_id() -> None:
    result = run_scenario("does-not-exist")
    assert result["status"] == "failed"
    assert "does-not-exist" in result["error"]
    assert "available_scenarios" in result


def test_run_scenario_error_lists_real_available_scenarios() -> None:
    from reliability.scenarios import REGISTRY

    result = run_scenario("does-not-exist")
    assert set(result["available_scenarios"]) == set(REGISTRY)


# ---------------------------------------------------------------------------
# run_scenario / run_all_scenarios: orchestration, with a fake registry so
# this doesn't require Spark/live infra to run fast
# ---------------------------------------------------------------------------


def test_run_scenario_writes_evidence_and_persists_outcome(tmp_path: Path) -> None:
    fake_registry = {"fake-scenario": _FakeScenarioModule("passed")}
    with (
        patch("reliability.runner.REGISTRY", fake_registry),
        patch("reliability.runner.persist_outcome_to_pipeline_run_log", return_value=True) as mock_persist,
    ):
        result = run_scenario("fake-scenario", artifacts_root=tmp_path)

    assert result["status"] == "passed"
    assert result["scenario_id"] == "fake-scenario"
    assert result["persisted_to_pipeline_run_log"] is True
    assert mock_persist.called
    evidence_dir = Path(result["evidence_dir"])
    assert evidence_dir.exists()
    assert (evidence_dir / "scenario.json").exists()


def test_run_scenario_reports_persist_failure_without_failing_the_scenario() -> None:
    """A pipeline_run_log write failure must not turn a passing scenario
    into a failed one — persistence is best-effort observability, not
    part of the scenario's own pass/fail contract.
    """
    fake_registry = {"fake-scenario": _FakeScenarioModule("passed")}
    with (
        patch("reliability.runner.REGISTRY", fake_registry),
        patch("reliability.runner.persist_outcome_to_pipeline_run_log", return_value=False),
    ):
        result = run_scenario("fake-scenario")

    assert result["status"] == "passed"
    assert result["persisted_to_pipeline_run_log"] is False


def test_run_all_scenarios_aggregates_failed_scenarios() -> None:
    fake_registry = {
        "fake-pass-1": _FakeScenarioModule("passed"),
        "fake-fail": _FakeScenarioModule("failed"),
        "fake-pass-2": _FakeScenarioModule("passed"),
    }
    with (
        patch("reliability.runner.REGISTRY", fake_registry),
        patch("reliability.runner.persist_outcome_to_pipeline_run_log", return_value=True),
    ):
        result = run_all_scenarios()

    assert result["status"] == "failed"
    assert result["failed_scenarios"] == ["fake-fail"]
    assert set(result["results"]) == {"fake-pass-1", "fake-fail", "fake-pass-2"}


def test_run_all_scenarios_passes_when_every_scenario_passes() -> None:
    fake_registry = {"fake-pass-1": _FakeScenarioModule("passed"), "fake-pass-2": _FakeScenarioModule("passed")}
    with (
        patch("reliability.runner.REGISTRY", fake_registry),
        patch("reliability.runner.persist_outcome_to_pipeline_run_log", return_value=True),
    ):
        result = run_all_scenarios()

    assert result["status"] == "passed"
    assert result["failed_scenarios"] == []


# ---------------------------------------------------------------------------
# persist_outcome_to_pipeline_run_log: best-effort, never raises
# ---------------------------------------------------------------------------


def test_persist_outcome_returns_true_on_success() -> None:
    def _close_and_return_none(coro):
        coro.close()
        return None

    result = _fake_result("passed")
    config = StreamingConfig()
    with patch("reliability.runner.run_coroutine", side_effect=_close_and_return_none) as mock_run:
        ok = persist_outcome_to_pipeline_run_log("fake-scenario", result, config)
    assert ok is True
    assert mock_run.called


def test_persist_outcome_returns_false_and_never_raises_on_db_failure() -> None:
    """A pipeline_run_log write failure (e.g. DB unreachable) must degrade
    to a logged warning and False, not propagate — matching the same
    best-effort principle already applied to spark/streaming/sinks.py's
    log_failure and lineage/events.py's emit_pipeline_lineage.
    """

    def _close_and_raise(coro):
        coro.close()
        raise ConnectionError("db unreachable")

    result = _fake_result("passed")
    config = StreamingConfig()
    with patch("reliability.runner.run_coroutine", side_effect=_close_and_raise):
        ok = persist_outcome_to_pipeline_run_log("fake-scenario", result, config)
    assert ok is False


def test_persist_outcome_writes_reliability_prefixed_pipeline_name() -> None:
    """Regression: the pipeline_name written must be `reliability:<id>` —
    this is the exact string services/ops-console/app/observability.py's
    fetch_reliability_status() pattern-matches on
    (`pipeline_name like 'reliability:%'`) to derive its Prometheus gauges.
    A drift here would silently break the affected components detection chain.
    Runs the real _persist_outcome coroutine (via run_coroutine, not
    mocked) against a FakePostgres double, so this actually exercises the
    SQL parameter binding, beyond the prefix constant in isolation.
    """
    import reliability.runner as runner_module

    calls: list[tuple[str, tuple]] = []

    class FakePostgres:
        def __init__(self, _database_url: str) -> None:
            pass

        async def execute(self, query: str, *params) -> None:
            calls.append((query, params))

        async def close(self) -> None:
            pass

    result = _fake_result("passed")
    config = StreamingConfig()
    with patch("platform_shared.database.Postgres", FakePostgres):
        ok = persist_outcome_to_pipeline_run_log("late-event", result, config)

    assert ok is True
    assert len(calls) == 1
    _query, params = calls[0]
    pipeline_name = params[0]
    assert pipeline_name == f"{runner_module.PIPELINE_NAME_PREFIX}late-event"
    assert params[1] == result.overall_status
