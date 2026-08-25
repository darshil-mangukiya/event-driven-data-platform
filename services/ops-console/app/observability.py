"""Derived Prometheus gauges, computed from the database on every /metrics scrape.

Some signals this platform wants observable don't have a natural
long-running process to emit them from in real time:

* **Reliability exercise outcomes** — ``platform_cli reliability run`` is a
  short-lived CLI process; see ``reliability/runner.py`` for why its
  outcome is persisted to ``pipeline_run_log`` instead of exposed from an
  ephemeral metrics server nothing would scrape in time.
* **Reconciliation results** — ``scripts/reconcile_metrics.py`` is also a
  short-lived CLI/cron-style script, writing to ``reconciliation_audit``.
* **Streaming checkpoint freshness, as seen from outside the streaming
  process** — useful specifically for the case where the Structured
  Streaming job's own ``/metrics`` (spark/streaming/metrics.py, scraped
  directly per ``monitoring/prometheus.yml``'s ``spark-streaming`` job)
  isn't reachable (the job crashed, the streaming Docker Compose profile
  isn't running) — the *database* still has the last commit's timestamp,
  so this gives operators a second, independent way to see the same kind
  of staleness.

``ops-console`` is already a long-running FastAPI service with a Postgres
connection and an existing ``/metrics`` endpoint (``generate_latest()`` off
the shared default registry) — this module just populates a few more
gauges into that same registry right before each scrape, rather than
inventing a second exporter process.

Label cardinality: every label here is a small, fixed enum (a handful of
scenario ids, or the four lateness classifications) — never a tenant_id,
event_id, trace_id, or free-text error string. See docs/observability.md's
"Label & Cardinality Strategy" for the platform-wide rule this follows.

Each ``refresh_*_gauges`` function is a thin wrapper over a ``fetch_*``
function that does the actual query and returns plain rows/dicts. This
split exists so the operator-facing HTML page (``app/main.py::ops_payload``,
the ops console can reuse the same queries to render its Streaming
Pipeline and Reliability Exercises sections, rather than maintaining a
second, drifting copy of the same SQL.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from platform_shared.database import Postgres
from prometheus_client import REGISTRY, Gauge

LOGGER = logging.getLogger(__name__)

RECONCILIATION_LOOKBACK_HOURS = 24
RELIABILITY_LOOKBACK_HOURS = 24 * 7  # exercises aren't expected to run continuously


def _gauge(name: str, documentation: str, labelnames: list[str] | None = None) -> Gauge:
    """Get-or-create a Gauge on the default registry.

    Test harnesses for this platform's services (tests/test_api_contracts.py,
    tests/test_ops_console.py) import each service's ``app.main`` fresh by
    deleting ``app.*`` from ``sys.modules`` and re-importing — necessary so
    switching between services doesn't return a different service's cached
    module. ops-console is the one service whose own ``app.*`` submodule
    (this one) defines module-level Prometheus metrics, so a naive
    ``Gauge(...)`` here would re-register and raise
    ``ValueError: Duplicated timeseries`` the second time any test imports
    this module in the same process. Reusing an already-registered
    collector makes re-import idempotent without weakening what's actually
    being tested.
    """
    existing = REGISTRY._names_to_collectors.get(name)  # noqa: SLF001 - documented, narrow, test-only concern
    if existing is not None:
        return existing  # type: ignore[return-value]
    return Gauge(name, documentation, labelnames or [])


RELIABILITY_LAST_STATUS = _gauge(
    "cloudscale_reliability_exercise_last_status",
    "1 if the most recent run of this reliability scenario passed, 0 if it failed.",
    ["scenario_id"],
)
RELIABILITY_LAST_RUN_AGE_SECONDS = _gauge(
    "cloudscale_reliability_exercise_last_run_age_seconds",
    "Seconds since the most recent run of this reliability scenario (from pipeline_run_log).",
    ["scenario_id"],
)
RECONCILIATION_RECENT_FAILURES = _gauge(
    "cloudscale_reconciliation_recent_failures",
    f"reconciliation_audit rows with status='failed' in the last {RECONCILIATION_LOOKBACK_HOURS}h.",
)
RECONCILIATION_RECENT_CHECKS = _gauge(
    "cloudscale_reconciliation_recent_checks",
    f"Total reconciliation_audit rows checked in the last {RECONCILIATION_LOOKBACK_HOURS}h.",
)
STREAM_CHECKPOINT_FRESHNESS_SECONDS = _gauge(
    "cloudscale_stream_checkpoint_freshness_seconds_db",
    "Seconds since the last streaming_checkpoint_audit commit, by query — read from "
    "the database so it stays available even when the streaming job's own /metrics "
    "endpoint isn't reachable.",
    ["query"],
)
STREAM_LATE_EVENTS_RECENT = _gauge(
    "cloudscale_stream_late_events_recent",
    f"streaming_late_events rows in the last {RECONCILIATION_LOOKBACK_HOURS}h, by classification.",
    ["classification"],
)
SERVING_METRICS_STALENESS_SECONDS = _gauge(
    "cloudscale_serving_metrics_staleness_seconds",
    "Seconds since the most recent update to a tenant-facing serving table, by table.",
    ["table"],
)


async def fetch_reliability_status(postgres: Postgres) -> list[dict]:
    """Most recent outcome per reliability scenario within the lookback
    window. Used both for the Prometheus gauges below and, directly, for
    the ops console's Reliability Exercises section.
    """
    return await postgres.fetch(
        """
        select distinct on (pipeline_name)
            substring(pipeline_name from length('reliability:') + 1) as scenario_id,
            status,
            finished_at
        from pipeline_run_log
        where pipeline_name like 'reliability:%'
          and finished_at is not null
          and started_at >= now() - ($1 || ' hours')::interval
        order by pipeline_name, finished_at desc
        """,
        str(RELIABILITY_LOOKBACK_HOURS),
    )


async def refresh_reliability_gauges(postgres: Postgres) -> None:
    rows = await fetch_reliability_status(postgres)
    now = datetime.now(timezone.utc)
    for row in rows:
        scenario_id = row["scenario_id"]
        RELIABILITY_LAST_STATUS.labels(scenario_id=scenario_id).set(1 if row["status"] == "passed" else 0)
        finished_at = row["finished_at"]
        age_seconds = max((now - finished_at).total_seconds(), 0.0) if finished_at else -1
        RELIABILITY_LAST_RUN_AGE_SECONDS.labels(scenario_id=scenario_id).set(age_seconds)


async def fetch_reconciliation_summary(postgres: Postgres) -> dict | None:
    return await postgres.fetchrow(
        """
        select
            count(*) filter (where status = 'failed') as failures,
            count(*) as total
        from reconciliation_audit
        where checked_at >= now() - ($1 || ' hours')::interval
        """,
        str(RECONCILIATION_LOOKBACK_HOURS),
    )


async def refresh_reconciliation_gauges(postgres: Postgres) -> None:
    row = await fetch_reconciliation_summary(postgres)
    if row is not None:
        RECONCILIATION_RECENT_FAILURES.set(row["failures"] or 0)
        RECONCILIATION_RECENT_CHECKS.set(row["total"] or 0)


async def fetch_checkpoint_freshness(postgres: Postgres) -> list[dict]:
    return await postgres.fetch(
        """
        select query_name, max(recorded_at) as last_commit
        from streaming_checkpoint_audit
        group by query_name
        order by query_name
        """
    )


async def refresh_checkpoint_freshness_gauges(postgres: Postgres) -> None:
    rows = await fetch_checkpoint_freshness(postgres)
    now = datetime.now(timezone.utc)
    for row in rows:
        last_commit = row["last_commit"]
        freshness = max((now - last_commit).total_seconds(), 0.0) if last_commit else -1
        STREAM_CHECKPOINT_FRESHNESS_SECONDS.labels(query=row["query_name"]).set(freshness)


async def fetch_late_events_summary(postgres: Postgres) -> list[dict]:
    return await postgres.fetch(
        """
        select classification, count(*) as recent_count
        from streaming_late_events
        where recorded_at >= now() - ($1 || ' hours')::interval
        group by classification
        order by classification
        """,
        str(RECONCILIATION_LOOKBACK_HOURS),
    )


async def refresh_late_events_gauges(postgres: Postgres) -> None:
    rows = await fetch_late_events_summary(postgres)
    for row in rows:
        STREAM_LATE_EVENTS_RECENT.labels(classification=row["classification"]).set(row["recent_count"])


async def fetch_serving_freshness(postgres: Postgres) -> list[dict]:
    """Freshness of the two tenant-facing serving tables — the async
    processing-service's ``tenant_metrics_daily`` and the Structured
    Streaming job's ``stream_window_metrics``. Table names are the only
    label — fixed, low cardinality, not per-tenant (a single slow tenant's
    metric shouldn't need a new label value; see per-tenant freshness in
    the demo dashboard / data-product docs instead).
    """
    now = datetime.now(timezone.utc)
    results: list[dict] = []
    for table in ("tenant_metrics_daily", "stream_window_metrics"):
        row = await postgres.fetchrow(f"select max(updated_at) as last_update from {table}")  # noqa: S608 - fixed allowlist, not user input
        last_update = row["last_update"] if row is not None else None
        staleness = max((now - last_update).total_seconds(), 0.0) if last_update else None
        results.append({"table": table, "last_update": last_update, "staleness_seconds": staleness})
    return results


async def refresh_serving_freshness_gauges(postgres: Postgres) -> None:
    rows = await fetch_serving_freshness(postgres)
    for row in rows:
        if row["staleness_seconds"] is not None:
            SERVING_METRICS_STALENESS_SECONDS.labels(table=row["table"]).set(row["staleness_seconds"])


async def refresh_all(postgres: Postgres) -> None:
    """Called right before every /metrics scrape. Each refresh is independent
    and best-effort — a missing table (e.g. migrations not yet applied) or a
    transient query failure in one shouldn't block the others or the scrape
    itself.
    """
    for refresh in (
        refresh_reliability_gauges,
        refresh_reconciliation_gauges,
        refresh_checkpoint_freshness_gauges,
        refresh_late_events_gauges,
        refresh_serving_freshness_gauges,
    ):
        try:
            await refresh(postgres)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("observability gauge refresh %s failed: %s", refresh.__name__, exc)
