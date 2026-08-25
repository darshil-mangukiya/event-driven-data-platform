"""Run one reliability scenario end-to-end: execute, write evidence, persist
the outcome for observability, return a summary.

Why persist to ``pipeline_run_log`` instead of a Prometheus counter: a
``platform_cli reliability run`` invocation is a short-lived CLI process —
it prints its result and exits well before Prometheus's next scrape
interval would ever see a counter it incremented. Rather than start a
metrics HTTP server nothing will scrape in time (that would be exactly the
"fake telemetry" the platform's conventions warn against), the outcome is
written to the existing ``pipeline_run_log`` table (already the platform's
generic "one row per pipeline/job run" ledger — reused here, not
duplicated) using ``pipeline_name = "reliability:<scenario_id>"``. The
long-running ``ops-console`` service then derives real, continuously
scraped Prometheus gauges from that table on every ``/metrics`` request —
see ``services/ops-console/app/observability.py``. That closes the loop:
failure injection -> telemetry signal -> alert/detection, described in
docs/reliability.md's "Reliability -> Detection Mapping".
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from reliability.evidence import DEFAULT_ARTIFACTS_ROOT, write_evidence
from reliability.injectors.asyncio_utils import run_coroutine
from reliability.models import ScenarioResult
from reliability.scenarios import REGISTRY
from spark.streaming.config import StreamingConfig

LOGGER = logging.getLogger(__name__)

PIPELINE_NAME_PREFIX = "reliability:"


def available_scenarios() -> list[str]:
    return sorted(REGISTRY.keys())


async def _persist_outcome(database_url: str, scenario_id: str, result: ScenarioResult) -> None:
    from platform_shared.database import Postgres

    postgres = Postgres(database_url)
    try:
        await postgres.execute(
            """
            insert into pipeline_run_log
                (pipeline_run_id, pipeline_name, status, records_processed, error_message, started_at, finished_at)
            values (gen_random_uuid(), $1, $2, $3, $4, $5, $6)
            """,
            f"{PIPELINE_NAME_PREFIX}{scenario_id}",
            result.overall_status,
            len(result.steps),
            None if result.overall_status != "failed" else result.observed_behavior[:2000],
            result.started_at,
            result.ended_at,
        )
    finally:
        await postgres.close()


def persist_outcome_to_pipeline_run_log(scenario_id: str, result: ScenarioResult, config: StreamingConfig) -> bool:
    """Best-effort. Returns True if the row was written, False otherwise (never raises)."""
    try:
        run_coroutine(_persist_outcome(config.database_url, scenario_id, result))
        return True
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("could not persist reliability outcome to pipeline_run_log: %s", exc)
        return False


def run_scenario(scenario_id: str, *, artifacts_root: Path | None = None) -> dict[str, Any]:
    if scenario_id not in REGISTRY:
        return {
            "status": "failed",
            "error": f"unknown scenario_id {scenario_id!r}",
            "available_scenarios": available_scenarios(),
        }
    module = REGISTRY[scenario_id]
    config = StreamingConfig.from_env()
    result = module.run(config)
    evidence_dir = write_evidence(result, artifacts_root=artifacts_root or DEFAULT_ARTIFACTS_ROOT)
    persisted = persist_outcome_to_pipeline_run_log(scenario_id, result, config)
    return {
        "status": result.overall_status,
        "validation_status": result.validation_status,
        "run_id": result.run_id,
        "scenario_id": scenario_id,
        "evidence_dir": str(evidence_dir),
        "persisted_to_pipeline_run_log": persisted,
        "steps": [{"name": s.name, "status": s.status, "detail": s.detail} for s in result.steps],
    }


def run_all_scenarios(*, artifacts_root: Path | None = None) -> dict[str, Any]:
    results = {scenario_id: run_scenario(scenario_id, artifacts_root=artifacts_root) for scenario_id in available_scenarios()}
    failed = [sid for sid, r in results.items() if r["status"] == "failed"]
    return {"status": "failed" if failed else "passed", "failed_scenarios": failed, "results": results}
