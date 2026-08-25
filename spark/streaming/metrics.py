"""Prometheus metrics for the Structured Streaming job.

Streaming jobs aren't a request/response service, so they can't reuse the
existing ``platform_shared.metrics`` Starlette middleware — instead this
module starts a small ``prometheus_client`` HTTP server in the Spark
*driver* process (one process, one port, configurable via
``STREAMING_METRICS_PORT``) that Prometheus scrapes directly, the same way
it scrapes any other long-running process. See
``monitoring/prometheus.yml`` for the added scrape target.

All counters, histograms, and gauges below are updated from runtime paths in
``sinks.py`` (inside ``foreachBatch``). Naming
follows the ``cloudscale_stream_*`` component convention,
which is deliberately distinct from the existing ``platform_*`` service
metrics namespace so the two are never confused when both show up in the
same Prometheus instance.
"""

from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

LOGGER = logging.getLogger(__name__)

STREAM_EVENTS_RECEIVED = Counter(
    "cloudscale_stream_events_received_total",
    "Raw events read from Kafka by the streaming job, before validation.",
    ["topic"],
)
STREAM_EVENTS_PROCESSED = Counter(
    "cloudscale_stream_events_processed_total",
    "Events that passed validation and were written to the serving layer.",
    ["event_domain"],
)
STREAM_EVENTS_FAILED = Counter(
    "cloudscale_stream_events_failed_total",
    "Events that failed contract validation and were routed to DLQ.",
    ["reason"],
)
STREAM_EVENTS_DUPLICATE = Counter(
    "cloudscale_stream_events_duplicate_total",
    "Duplicate events detected within a micro-batch (tenant_id, event_id).",
    ["event_domain"],
)
STREAM_EVENTS_LATE = Counter(
    "cloudscale_stream_events_late_total",
    "Events classified as late-arriving, by classification.",
    ["classification"],
)
STREAM_DLQ_TOTAL = Counter(
    "cloudscale_stream_dlq_total",
    "Events written to the Kafka DLQ topic by the streaming job.",
    ["reason"],
)
STREAM_BATCHES_TOTAL = Counter(
    "cloudscale_stream_batches_total",
    "Micro-batches processed, by query and outcome.",
    ["query", "outcome"],
)
STREAM_BATCH_DURATION = Histogram(
    "cloudscale_stream_batch_duration_seconds",
    "Wall-clock duration of a micro-batch's sink write.",
    ["query"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
STREAM_PROCESSING_LAG = Gauge(
    "cloudscale_stream_processing_lag_seconds",
    "Approximate processing lag: batch completion time minus max event_timestamp in the batch.",
    ["query"],
)
STREAM_WATERMARK_LAG = Gauge(
    "cloudscale_stream_watermark_lag_seconds",
    "Gap between the newest event_timestamp seen and current processing time.",
    ["query"],
)
STREAM_SINK_FAILURES = Counter(
    "cloudscale_stream_sink_failures_total",
    "Sink write failures (e.g. PostgreSQL unavailable), after retries exhausted.",
    ["sink"],
)
STREAM_CHECKPOINT_AGE = Gauge(
    "cloudscale_stream_checkpoint_age_seconds",
    "Unix timestamp of the last successful checkpoint commit, by query. "
    "Alert on time() - this value to get actual staleness in seconds "
    "(see monitoring/alert_rules.yml::StreamingCheckpointStale).",
    ["query"],
)
STREAM_RECORDS_PER_BATCH = Histogram(
    "cloudscale_stream_records_per_batch",
    "Number of records written by a single micro-batch's sink call, by query.",
    ["query"],
    buckets=(0, 1, 5, 10, 50, 100, 500, 1000, 5000),
)
STREAM_POSTGRES_AVAILABLE = Gauge(
    "cloudscale_stream_postgres_available",
    "1 if the last PostgresSink write succeeded, 0 if it exhausted its retries "
    "(see spark.streaming.sinks._with_retries).",
)

_server_started = False


def start_metrics_server(port: int) -> None:
    """Idempotently start the Prometheus HTTP exporter in this process."""
    global _server_started
    if _server_started:
        return
    try:
        start_http_server(port)
        _server_started = True
        LOGGER.info("streaming metrics server listening on :%s/metrics", port)
    except OSError as exc:
        # Common in local dev when re-running the job in the same process
        # (e.g. repeated pytest runs) — don't crash the pipeline over it.
        LOGGER.warning("could not start metrics server on port %s: %s", port, exc)
