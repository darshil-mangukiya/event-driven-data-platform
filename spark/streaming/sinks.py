"""foreachBatch sinks: idempotent, retried micro-batch writes into PostgreSQL.

PostgreSQL remains the platform's serving layer (per the architecture
requirements — no Delta Lake, no second serving system). Each sink here is
called from a ``foreachBatch`` callback on an already-materialized,
static micro-batch DataFrame, and writes through ``psycopg2`` using
``ON CONFLICT`` upserts so re-processing a batch (Spark's at-least-once
``foreachBatch`` semantics, or a manual restart from an older checkpoint)
never double-counts a window's metrics or duplicates an audit row.

Failure handling: every write is wrapped in :func:`_with_retries`, which
retries transient failures (e.g. a momentarily unavailable PostgreSQL —
see the ``db-outage`` reliability exercise) with a short backoff. If all
retries are exhausted, the failure is recorded in ``streaming_failures``
*on a fresh connection* (so a dead pool doesn't also swallow the failure
record) and re-raised — Spark then fails the batch and retries it from the
same offsets on the next trigger, rather than silently losing data.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
from pyspark.sql import DataFrame

from spark.streaming.config import StreamingConfig
from spark.streaming.metrics import (
    STREAM_BATCH_DURATION,
    STREAM_CHECKPOINT_AGE,
    STREAM_EVENTS_LATE,
    STREAM_POSTGRES_AVAILABLE,
    STREAM_RECORDS_PER_BATCH,
    STREAM_SINK_FAILURES,
)

LOGGER = logging.getLogger(__name__)


class SinkError(RuntimeError):
    """Raised when a sink write fails after exhausting retries."""


def _with_retries(config: StreamingConfig, sink_name: str, fn: Callable[[], None]) -> None:
    last_exc: Exception | None = None
    for attempt in range(1, config.sink_max_retries + 1):
        try:
            fn()
            STREAM_POSTGRES_AVAILABLE.set(1)
            return
        except Exception as exc:  # noqa: BLE001 - deliberately broad, this is the retry boundary
            last_exc = exc
            LOGGER.warning(
                "sink '%s' write failed (attempt %s/%s): %s",
                sink_name,
                attempt,
                config.sink_max_retries,
                exc,
            )
            if attempt < config.sink_max_retries:
                time.sleep(config.sink_retry_backoff_seconds * attempt)
    STREAM_SINK_FAILURES.labels(sink=sink_name).inc()
    STREAM_POSTGRES_AVAILABLE.set(0)
    assert last_exc is not None
    raise SinkError(f"sink '{sink_name}' failed after {config.sink_max_retries} attempts") from last_exc


def _connect(config: StreamingConfig):
    return psycopg2.connect(config.database_url, connect_timeout=config.sink_connect_timeout_seconds)


class PostgresSink:
    """Owns the DB writes for one streaming job run."""

    def __init__(self, config: StreamingConfig, run_id: str | None = None) -> None:
        self.config = config
        self.run_id = run_id or str(uuid.uuid4())

    # -- run bookkeeping -----------------------------------------------

    def start_run(self, job_name: str, config_snapshot: dict[str, Any]) -> None:
        import json

        def _do() -> None:
            with _connect(self.config) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    insert into stream_processing_runs
                        (run_id, job_name, status, config_snapshot, started_at)
                    values (%s, %s, 'running', %s, now())
                    on conflict (run_id) do nothing
                    """,
                    (self.run_id, job_name, json.dumps(config_snapshot)),
                )

        _with_retries(self.config, "stream_processing_runs", _do)
        self._write_lineage_event(job_name=job_name, status="running")

    def finish_run(self, status: str) -> None:
        def _do() -> None:
            with _connect(self.config) as conn, conn.cursor() as cur:
                cur.execute(
                    "update stream_processing_runs set status = %s, ended_at = now() where run_id = %s",
                    (status, self.run_id),
                )

        _with_retries(self.config, "stream_processing_runs", _do)
        self._write_lineage_event(job_name="cloudscale-structured-streaming", status=status)

    def _write_lineage_event(self, *, job_name: str, status: str) -> None:
        """Best-effort, sync (psycopg2) equivalent of
        lineage.events.emit_pipeline_lineage — the async version can't be
        used here since PostgresSink is called from Spark's driver thread
        via foreachBatch, not an asyncio event loop. Correlated by the same
        run_id as stream_processing_runs, closing the same
        pipeline_run_log/lineage_events correlation gap
        scripts/backfill_metrics.py and scripts/reconcile_metrics.py close
        on the async side.
        """
        import json

        try:
            with _connect(self.config) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    insert into lineage_events (
                        event_type, job_name, run_id, tenant_id, input_datasets,
                        output_datasets, status, event_timestamp, metadata
                    )
                    values (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, now(), %s::jsonb)
                    """,
                    (
                        "COMPLETE" if status in ("running", "completed", "stopped") else "FAIL",
                        job_name,
                        self.run_id,
                        None,
                        json.dumps([{"namespace": "kafka", "name": topic} for topic in self.config.subscribe_topics]),
                        json.dumps([{"namespace": "postgres", "name": "stream_window_metrics"}]),
                        status,
                        json.dumps({"source": "spark.streaming.sinks.PostgresSink"}),
                    ),
                )
        except Exception:  # noqa: BLE001
            LOGGER.warning("could not write lineage_events row for run %s (status=%s)", self.run_id, status)

    def log_failure(self, *, batch_id: int, tenant_id: str | None, stage: str, error_message: str) -> None:
        """Best-effort failure logging on a fresh connection; never raises."""
        try:
            with _connect(self.config) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    insert into streaming_failures
                        (failure_id, run_id, batch_id, tenant_id, stage, error_message, recorded_at)
                    values (%s, %s, %s, %s, %s, %s, now())
                    """,
                    (str(uuid.uuid4()), self.run_id, batch_id, tenant_id, stage, error_message[:2000]),
                )
        except Exception:  # noqa: BLE001
            LOGGER.exception("failed to record streaming_failures row (batch_id=%s, stage=%s)", batch_id, stage)

    # -- per-batch sinks -------------------------------------------------

    def write_window_metrics(self, batch_df: DataFrame, batch_id: int) -> int:
        rows = [
            (
                row["tenant_id"],
                row["window_start"],
                row["window_end"],
                row["event_domain"],
                row["metric_name"],
                row["metric_value"],
                row["event_count"],
            )
            for row in batch_df.collect()
        ]
        if not rows:
            return 0

        def _do() -> None:
            with _connect(self.config) as conn, conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    insert into stream_window_metrics
                        (tenant_id, window_start, window_end, event_domain, metric_name,
                         metric_value, event_count, batch_id, updated_at)
                    values %s
                    on conflict (tenant_id, window_start, window_end, event_domain, metric_name)
                    do update set
                        metric_value = excluded.metric_value,
                        event_count = excluded.event_count,
                        batch_id = excluded.batch_id,
                        updated_at = now()
                    """,
                    [(*row, batch_id, datetime.now(timezone.utc)) for row in rows],
                )

        start = time.monotonic()
        _with_retries(self.config, "stream_window_metrics", _do)
        STREAM_BATCH_DURATION.labels(query="aggregates").observe(time.monotonic() - start)
        STREAM_RECORDS_PER_BATCH.labels(query="aggregates").observe(len(rows))
        return len(rows)

    def write_late_events(self, batch_df: DataFrame, batch_id: int) -> int:
        rows = [
            (
                row["event_id"],
                row["tenant_id"],
                row["event_type"],
                row["event_domain"],
                row["event_timestamp"],
                row["kafka_ingest_timestamp"],
                row["lateness_seconds"],
                row["lateness_classification"],
            )
            for row in batch_df.collect()
            if row["event_id"] is not None
        ]
        if not rows:
            return 0

        def _do() -> None:
            with _connect(self.config) as conn, conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    insert into streaming_late_events
                        (event_id, tenant_id, event_type, event_domain, event_timestamp,
                         ingestion_timestamp, lateness_seconds, classification, batch_id, recorded_at)
                    values %s
                    on conflict (tenant_id, event_id) do nothing
                    """,
                    [(*row, batch_id, datetime.now(timezone.utc)) for row in rows],
                )

        _with_retries(self.config, "streaming_late_events", _do)
        for row in rows:
            STREAM_EVENTS_LATE.labels(classification=row[7]).inc()
        STREAM_RECORDS_PER_BATCH.labels(query="late-events").observe(len(rows))
        return len(rows)

    def write_checkpoint_audit(
        self,
        *,
        query_name: str,
        batch_id: int,
        checkpoint_location: str,
        input_rows: int,
        batch_duration_ms: float,
    ) -> None:
        def _do() -> None:
            with _connect(self.config) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    insert into streaming_checkpoint_audit
                        (run_id, query_name, batch_id, checkpoint_location, input_rows,
                         batch_duration_ms, recorded_at)
                    values (%s, %s, %s, %s, %s, %s, now())
                    """,
                    (self.run_id, query_name, batch_id, checkpoint_location, input_rows, batch_duration_ms),
                )

        _with_retries(self.config, "streaming_checkpoint_audit", _do)
        # Store the wall-clock time of this commit, not an "age" of 0 — the
        # Prometheus alert (StreamingCheckpointStale) computes
        # `time() - cloudscale_stream_checkpoint_age_seconds` itself, so the
        # gauge needs to hold a timestamp for that subtraction to mean
        # anything once the query stops committing.
        STREAM_CHECKPOINT_AGE.labels(query=query_name).set(time.time())

    def write_watermark(self, *, batch_id: int, event_time_watermark) -> None:
        def _do() -> None:
            with _connect(self.config) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    insert into streaming_watermarks (run_id, batch_id, event_time_watermark, recorded_at)
                    values (%s, %s, %s, now())
                    """,
                    (self.run_id, batch_id, event_time_watermark),
                )

        _with_retries(self.config, "streaming_watermarks", _do)


def collect_reason_counts(df: DataFrame, column: str) -> Iterable[tuple[str, int]]:
    """Small helper: (value, count) pairs for a column, for metrics/logging."""
    counted = df.groupBy(column).count().collect()
    return [(row[column], row["count"]) for row in counted if row[column] is not None]
