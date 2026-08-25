"""Environment-driven configuration for the Structured Streaming job.

Mirrors the style of ``services/shared/platform_shared/config.py`` (frozen
dataclass + ``from_env`` + ``validate``) so the streaming job feels like a
natural extension of the existing platform rather than a parallel
configuration system.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

DEFAULT_TOPICS = (
    "platform.events.orders",
    "platform.events.payments",
    "platform.events.users",
    "platform.events.products",
    "platform.events.system",
)

# Supported event contract versions per domain. Extending this list is how a
# new payload version becomes acceptable to the streaming layer; anything
# outside these bounds is routed to the invalid/DLQ path (see validation.py).
SUPPORTED_PAYLOAD_VERSIONS = (1, 2)


def _as_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _as_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _as_tuple(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return tuple(item.strip() for item in value.split(",") if item.strip())


@dataclass(frozen=True)
class StreamingConfig:
    """All tunables for the streaming job, sourced from the environment.

    Nothing in the pipeline modules should read ``os.environ`` directly;
    everything flows through an instance of this class so job behavior is
    reproducible and testable.
    """

    environment: str = "local"
    app_name: str = "cloudscale-structured-streaming"

    # Kafka source
    kafka_bootstrap_servers: str = "localhost:9092"
    subscribe_topics: tuple[str, ...] = DEFAULT_TOPICS
    starting_offsets: str = "latest"
    max_offsets_per_trigger: int | None = None

    # Trigger / execution
    trigger_processing_time: str = "30 seconds"
    output_mode_events: str = "append"
    output_mode_aggregates: str = "update"

    # Event-time semantics
    watermark_delay: str = "10 minutes"
    # Events later than this (but within the watermark) are still processed
    # but flagged as "late_accepted" for observability.
    late_accept_threshold_seconds: int = 60
    # Events later than this are classified "late_rejected": recorded for
    # audit/reconciliation but excluded from windowed aggregates.
    #
    # IMPORTANT: this should be <= the watermark_delay expressed in seconds.
    # Spark silently drops any row older than the watermark before it ever
    # reaches the aggregation stage, so "late_accepted" only means something
    # (the row still contributes to a window) as long as the
    # reject threshold sits at or inside the watermark boundary. Default
    # watermark_delay is "10 minutes" -> 600s, matched here.
    late_reject_threshold_seconds: int = 600

    # Windowing
    window_duration: str = "5 minutes"
    window_slide_duration: str | None = None

    # Checkpointing — each query gets its own subdirectory so independent
    # streaming queries (events/DLQ/aggregates) never collide.
    checkpoint_root: str = "/tmp/spark/checkpoints/cloudscale-streaming"

    # Tenant scoping — empty tuple means "all tenants"
    tenant_filter: tuple[str, ...] = field(default_factory=tuple)

    # Serving sink
    database_url: str = "postgresql://platform:platform@localhost:5432/data_platform"
    jdbc_url: str = "jdbc:postgresql://localhost:5432/data_platform"
    # Not the PostgreSQL superuser `platform` — a NOSUPERUSER, dedicated
    # background role (see database/security/tenant_rls.sql and
    # evidence/validation/application-rls-runtime-verification.md).
    jdbc_user: str = "platform_admin_bypass"
    jdbc_password: str = "local-admin-bypass-change-me"
    sink_max_retries: int = 3
    sink_retry_backoff_seconds: float = 0.5
    sink_connect_timeout_seconds: int = 5

    # DLQ
    dlq_topic: str = "platform.events.dlq"

    # Observability
    metrics_port: int = 8007
    enable_metrics_server: bool = True

    @classmethod
    def from_env(cls) -> StreamingConfig:
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        db_url = os.getenv(
            "DATABASE_URL", "postgresql://platform:platform@localhost:5432/data_platform"
        )
        jdbc_url = os.getenv("STREAMING_JDBC_URL") or _database_url_to_jdbc(db_url)
        max_offsets = os.getenv("STREAMING_MAX_OFFSETS_PER_TRIGGER")
        return cls(
            environment=os.getenv("ENVIRONMENT", "local"),
            app_name=os.getenv("STREAMING_APP_NAME", "cloudscale-structured-streaming"),
            kafka_bootstrap_servers=bootstrap,
            subscribe_topics=_as_tuple("STREAMING_SUBSCRIBE_TOPICS", DEFAULT_TOPICS),
            starting_offsets=os.getenv("STREAMING_STARTING_OFFSETS", "latest"),
            max_offsets_per_trigger=int(max_offsets) if max_offsets else None,
            trigger_processing_time=os.getenv("STREAMING_TRIGGER_INTERVAL", "30 seconds"),
            output_mode_events=os.getenv("STREAMING_OUTPUT_MODE_EVENTS", "append"),
            output_mode_aggregates=os.getenv("STREAMING_OUTPUT_MODE_AGGREGATES", "update"),
            watermark_delay=os.getenv("STREAMING_WATERMARK_DELAY", "10 minutes"),
            late_accept_threshold_seconds=_as_int("STREAMING_LATE_ACCEPT_THRESHOLD_SECONDS", 60),
            late_reject_threshold_seconds=_as_int(
                "STREAMING_LATE_REJECT_THRESHOLD_SECONDS", 600
            ),
            window_duration=os.getenv("STREAMING_WINDOW_DURATION", "5 minutes"),
            window_slide_duration=os.getenv("STREAMING_WINDOW_SLIDE_DURATION") or None,
            checkpoint_root=os.getenv(
                "STREAMING_CHECKPOINT_ROOT", "/tmp/spark/checkpoints/cloudscale-streaming"
            ),
            tenant_filter=_as_tuple("STREAMING_TENANT_FILTER", ()),
            database_url=db_url,
            jdbc_url=jdbc_url,
            jdbc_user=os.getenv("STREAMING_JDBC_USER", "platform_admin_bypass"),
            jdbc_password=os.getenv("STREAMING_JDBC_PASSWORD", "local-admin-bypass-change-me"),
            sink_max_retries=_as_int("STREAMING_SINK_MAX_RETRIES", 3),
            sink_retry_backoff_seconds=float(os.getenv("STREAMING_SINK_RETRY_BACKOFF_SECONDS", "0.5")),
            sink_connect_timeout_seconds=_as_int("STREAMING_SINK_CONNECT_TIMEOUT_SECONDS", 5),
            dlq_topic=os.getenv("STREAMING_DLQ_TOPIC", "platform.events.dlq"),
            metrics_port=_as_int("STREAMING_METRICS_PORT", 8007),
            enable_metrics_server=_as_bool("STREAMING_ENABLE_METRICS_SERVER", True),
        )

    def checkpoint_path(self, query_name: str) -> str:
        """Per-query checkpoint directory, deterministic and collision-free."""
        return f"{self.checkpoint_root.rstrip('/')}/{query_name}"


def _database_url_to_jdbc(database_url: str) -> str:
    """Convert a ``postgresql://user:pass@host:port/db`` URL into a JDBC URL.

    Credentials are passed separately as connection properties, not embedded
    in the URL, matching standard Spark JDBC sink usage.
    """
    remainder = database_url.split("://", 1)[-1]
    if "@" in remainder:
        _, host_and_db = remainder.split("@", 1)
    else:
        host_and_db = remainder
    return f"jdbc:postgresql://{host_and_db}"


def interval_to_seconds(interval: str) -> int:
    """Parse a Spark interval string like '10 minutes' or '30 seconds'."""
    parts = interval.strip().split()
    if len(parts) != 2:
        raise ValueError(f"Unrecognized interval format: {interval!r}")
    amount, unit = parts
    unit = unit.lower().rstrip("s")
    multiplier = {"second": 1, "minute": 60, "hour": 3600, "day": 86400}.get(unit)
    if multiplier is None:
        raise ValueError(f"Unsupported interval unit: {unit!r}")
    return int(float(amount) * multiplier)


def validate_config(config: StreamingConfig) -> list[str]:
    errors: list[str] = []
    if not config.kafka_bootstrap_servers:
        errors.append("kafka_bootstrap_servers must not be empty")
    if not config.subscribe_topics:
        errors.append("subscribe_topics must contain at least one topic")
    if config.starting_offsets not in {"latest", "earliest"} and not config.starting_offsets.startswith("{"):
        errors.append("starting_offsets must be 'latest', 'earliest', or a JSON offsets map")
    if config.late_accept_threshold_seconds < 0:
        errors.append("late_accept_threshold_seconds must be >= 0")
    if config.late_reject_threshold_seconds <= config.late_accept_threshold_seconds:
        errors.append("late_reject_threshold_seconds must be greater than late_accept_threshold_seconds")
    try:
        watermark_seconds = interval_to_seconds(config.watermark_delay)
        if config.late_reject_threshold_seconds > watermark_seconds:
            errors.append(
                "late_reject_threshold_seconds must be <= watermark_delay "
                f"({watermark_seconds}s) or Spark will drop 'late_accepted' rows "
                "before they reach aggregation"
            )
    except ValueError as exc:
        errors.append(f"watermark_delay is invalid: {exc}")
    if config.sink_max_retries < 1:
        errors.append("sink_max_retries must be >= 1")
    if config.metrics_port <= 0 or config.metrics_port > 65535:
        errors.append("metrics_port must be a valid TCP port")
    if not config.checkpoint_root:
        errors.append("checkpoint_root must not be empty")
    return errors
