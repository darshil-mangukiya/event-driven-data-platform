"""Real lineage-event emission, wired into actual pipeline runs.

Previously, ``lineage_events`` was only ever written by manually
invoking ``scripts/emit_lineage_event.py`` from the command line — no
pipeline (backfill, reconciliation, the Structured Streaming job) actually
emitted a lineage event as part of its own run. This module closes that
gap by giving real pipeline code a single call that builds and persists an
event correlated by the *same run_id* that pipeline already uses for its
own ``pipeline_run_log`` (or ``stream_processing_runs``) row — the
"correlate pipeline_run_log, lineage_events... by run ID" goal
``docs/openlineage-tracking.md`` already described as a "production
evolution" item.

Reuses ``scripts/emit_lineage_event.py``'s ``build_lineage_event`` and
``persist_lineage_event`` rather than reimplementing them — this module is
the *caller*, not a second implementation.
"""

from __future__ import annotations

import logging
from typing import Any

from platform_shared.database import Postgres

from scripts.emit_lineage_event import build_lineage_event, insert_lineage_event

LOGGER = logging.getLogger(__name__)


async def emit_pipeline_lineage(
    postgres: Postgres,
    *,
    job_name: str,
    run_id: str,
    tenant_id: str | None,
    inputs: list[str],
    outputs: list[str],
    status: str,
    metadata: dict[str, Any] | None = None,
) -> bool:
    """Best-effort: build and persist a lineage event for one pipeline run.

    Returns True if persisted, False if it failed — never raises. A
    lineage-event write failing must not fail the pipeline run itself
    (the same "observability must not become a new failure mode"
    principle already applied to ``spark/streaming/sinks.py::log_failure``
    and ``reliability/runner.py::persist_outcome_to_pipeline_run_log``).
    """
    event = build_lineage_event(
        job_name=job_name,
        run_id=run_id,
        tenant_id=tenant_id,
        inputs=inputs,
        outputs=outputs,
        status=status,
        metadata=metadata,
    )
    try:
        await insert_lineage_event(postgres, event)
        return True
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("could not persist lineage event for %s run %s: %s", job_name, run_id, exc)
        return False
