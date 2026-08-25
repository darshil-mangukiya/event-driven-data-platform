from __future__ import annotations

from alembic import op

revision = "0006_streaming_serving_tables"
down_revision = "0005_traceability_watermarks_audit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        -- One row per Structured Streaming job invocation (driver process
        -- lifetime). Lets an operator see when the job last ran, with what
        -- config, and whether it exited cleanly.
        create table if not exists stream_processing_runs (
            run_id uuid primary key,
            job_name text not null,
            status text not null default 'running',
            config_snapshot jsonb not null default '{}'::jsonb,
            started_at timestamptz not null default now(),
            ended_at timestamptz
        );

        create index if not exists idx_stream_processing_runs_job_started
            on stream_processing_runs (job_name, started_at desc);

        -- Windowed business aggregates (revenue, orders, payment health,
        -- throughput) produced by the Structured Streaming aggregation
        -- query. Kept as one tidy long-format table instead of one table
        -- per metric to avoid schema bloat.
        create table if not exists stream_window_metrics (
            tenant_id text not null references tenant_config(tenant_id),
            window_start timestamptz not null,
            window_end timestamptz not null,
            event_domain text not null,
            metric_name text not null,
            metric_value double precision not null default 0,
            event_count integer not null default 0,
            batch_id bigint not null,
            updated_at timestamptz not null default now(),
            primary key (tenant_id, window_start, window_end, event_domain, metric_name)
        );

        create index if not exists idx_stream_window_metrics_tenant_time
            on stream_window_metrics (tenant_id, window_start desc);
        create index if not exists idx_stream_window_metrics_metric
            on stream_window_metrics (metric_name, window_start desc);

        -- Spark's event-time watermark value per micro-batch. Distinct from
        -- the existing pipeline_watermarks table (which tracks per-tenant,
        -- per-topic consumer offsets for the async processing-service) —
        -- this tracks the streaming *query's* global event-time watermark,
        -- used to reason about state-cleanup and late-data boundaries.
        create table if not exists streaming_watermarks (
            run_id uuid not null references stream_processing_runs(run_id),
            batch_id bigint not null,
            event_time_watermark timestamptz,
            recorded_at timestamptz not null default now(),
            primary key (run_id, batch_id)
        );

        -- One row per micro-batch commit, for checkpoint/recovery evidence.
        create table if not exists streaming_checkpoint_audit (
            run_id uuid not null references stream_processing_runs(run_id),
            query_name text not null,
            batch_id bigint not null,
            checkpoint_location text not null,
            input_rows integer not null default 0,
            batch_duration_ms double precision,
            recorded_at timestamptz not null default now(),
            primary key (run_id, query_name, batch_id)
        );

        -- Sink/stage failures, so a DB or Redis outage during streaming
        -- leaves an evidence trail instead of a silently-stalled job.
        create table if not exists streaming_failures (
            failure_id uuid primary key,
            run_id uuid not null references stream_processing_runs(run_id),
            batch_id bigint,
            tenant_id text,
            stage text not null,
            error_message text not null,
            recorded_at timestamptz not null default now()
        );

        create index if not exists idx_streaming_failures_run
            on streaming_failures (run_id, recorded_at desc);

        -- Per-event late-arrival audit trail (on_time / late_accepted /
        -- late_rejected). late_rejected rows are excluded from
        -- stream_window_metrics aggregation but still recorded here for
        -- reconciliation.
        create table if not exists streaming_late_events (
            event_id text not null,
            tenant_id text not null references tenant_config(tenant_id),
            event_type text not null,
            event_domain text,
            event_timestamp timestamptz,
            ingestion_timestamp timestamptz,
            lateness_seconds bigint,
            classification text not null,
            batch_id bigint not null,
            recorded_at timestamptz not null default now(),
            primary key (tenant_id, event_id)
        );

        create index if not exists idx_streaming_late_events_classification
            on streaming_late_events (tenant_id, classification, recorded_at desc);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        drop index if exists idx_streaming_late_events_classification;
        drop table if exists streaming_late_events;

        drop index if exists idx_streaming_failures_run;
        drop table if exists streaming_failures;

        drop table if exists streaming_checkpoint_audit;
        drop table if exists streaming_watermarks;

        drop index if exists idx_stream_window_metrics_metric;
        drop index if exists idx_stream_window_metrics_tenant_time;
        drop table if exists stream_window_metrics;

        drop index if exists idx_stream_processing_runs_job_started;
        drop table if exists stream_processing_runs;
        """
    )
