"""PostgreSQL-outage reliability exercise.

Points the real streaming sink (``spark.streaming.sinks.PostgresSink``) at
a deliberately unreachable database — a safe way to produce a genuine
connection failure without touching any real database — and proves the
retry/backoff/failure-handling path actually executes: bounded retries,
measurable backoff, and a raised ``SinkError`` (which fails the Spark batch
so the checkpoint isn't advanced and the same offsets are retried) rather
than a silent, unrecoverable stall or silent data loss.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from reliability.injectors.reachability import postgres_reachable
from reliability.models import ScenarioResult, StepResult
from spark.streaming.config import StreamingConfig

SCENARIO_ID = "db-outage"

# Deliberately unreachable, same rationale as redis_outage.py's TEST-NET-1 address.
UNREACHABLE_DATABASE_URL = "postgresql://platform:platform@192.0.2.1:5432/data_platform"


def run(config: StreamingConfig | None = None) -> ScenarioResult:
    from spark.streaming.sinks import PostgresSink, SinkError

    config = config or StreamingConfig(
        database_url=UNREACHABLE_DATABASE_URL,
        sink_max_retries=3,
        sink_retry_backoff_seconds=0.3,
        sink_connect_timeout_seconds=2,
    )
    steps: list[StepResult] = []

    sink = PostgresSink(config)
    started = time.monotonic()
    raised_sink_error = False
    try:
        sink.write_watermark(batch_id=1, event_time_watermark=datetime.now(timezone.utc))
    except SinkError:
        raised_sink_error = True
    except Exception as exc:  # noqa: BLE001
        steps.append(
            StepResult(
                name="write_fails_with_bounded_retries",
                status="failed",
                detail=f"expected spark.streaming.sinks.SinkError, got {type(exc).__name__}: {exc}",
            )
        )
    elapsed = time.monotonic() - started

    if raised_sink_error:
        # backoff is sink_retry_backoff_seconds * attempt for attempts 1..(N-1),
        # so total sleep is roughly backoff * (1 + 2 + ... + (N-1)).
        min_expected_elapsed = config.sink_retry_backoff_seconds * sum(range(1, config.sink_max_retries))
        ok = elapsed >= min_expected_elapsed
        steps.append(
            StepResult(
                name="write_fails_with_bounded_retries",
                status="verified" if ok else "failed",
                detail=(
                    f"write_watermark against an unreachable database raised SinkError after "
                    f"{config.sink_max_retries} attempts in {elapsed:.2f}s (expected >= {min_expected_elapsed:.2f}s of backoff)"
                ),
                evidence={"elapsed_seconds": round(elapsed, 3), "max_retries": config.sink_max_retries},
            )
        )

    # log_failure is best-effort and must never raise, even against the
    # same unreachable database — the failure record itself may not
    # persist during a real outage, but the caller must not crash over it.
    try:
        sink.log_failure(batch_id=1, tenant_id="tenant_demo", stage="reliability-exercise", error_message="db outage drill")
        steps.append(
            StepResult(
                name="log_failure_never_raises",
                status="verified",
                detail="PostgresSink.log_failure completed without raising, even though the database it tries to write to is also unreachable",
            )
        )
    except Exception as exc:  # noqa: BLE001
        steps.append(StepResult(name="log_failure_never_raises", status="failed", detail=f"log_failure raised: {exc}"))

    real_db_up = postgres_reachable(config.database_url if config.database_url != UNREACHABLE_DATABASE_URL else "postgresql://platform:platform@localhost:5432/data_platform")
    steps.append(
        StepResult(
            name="baseline_check_local_postgres",
            status="verified",
            detail=f"local PostgreSQL reachable={real_db_up} (informational only, not part of the injected failure)",
            evidence={"reachable": real_db_up},
        )
    )

    result = ScenarioResult(
        scenario_id=SCENARIO_ID,
        title="PostgreSQL Outage",
        component="spark.streaming.sinks.PostgresSink",
        expected_behavior=(
            "A temporarily unreachable PostgreSQL must not cause silent data loss: the sink retries "
            "a bounded number of times with backoff, then raises so the Spark batch fails and its "
            "offsets are not committed — the same micro-batch is retried on the next trigger once the "
            "database recovers, not dropped."
        ),
        detection_method="SinkError raised -> cloudscale_stream_sink_failures_total increments -> streaming_failures logged (best-effort, on a fresh connection) -> Spark batch marked failed.",
        impact=(
            "Without bounded retries, a single transient DB blip could either hang the pipeline "
            "indefinitely or silently drop a batch's writes. With them, the pipeline blocks briefly, "
            "surfaces the failure, and recovers automatically once PostgreSQL is reachable again."
        ),
        root_cause="Network partition, PostgreSQL restart/failover, or connection pool exhaustion.",
        recovery="No manual recovery needed for a transient outage — the next trigger's foreachBatch call retries the same offsets once PostgreSQL is reachable.",
        corrective_action="None required for a transient outage; a sustained outage should page via the sink-failure-rate alert (see docs/observability.md).",
        preventive_control="_with_retries wraps every PostgresSink write (sinks.py); STREAMING_SINK_MAX_RETRIES/STREAMING_SINK_RETRY_BACKOFF_SECONDS are configurable per environment.",
        steps=steps,
    )
    result.ended_at = datetime.now(timezone.utc)
    return result
