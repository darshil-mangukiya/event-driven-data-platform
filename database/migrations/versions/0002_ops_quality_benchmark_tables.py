from __future__ import annotations

from alembic import op

revision = "0002_ops_quality_benchmark_tables"
down_revision = "0001_initial_platform_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        create table if not exists dlq_replay_audit (
            replay_id uuid primary key default gen_random_uuid(),
            original_event_id text,
            tenant_id text,
            source_topic text not null,
            target_topic text,
            replay_status text not null,
            replay_reason text,
            replayed_by text not null default 'local-operator',
            replayed_at timestamptz not null default now(),
            error_message text
        );

        create table if not exists data_quality_check_results (
            check_result_id uuid primary key default gen_random_uuid(),
            check_name text not null,
            check_category text not null,
            tenant_id text,
            status text not null,
            severity text not null,
            observed_value numeric,
            threshold_value numeric,
            details jsonb not null default '{}'::jsonb,
            checked_at timestamptz not null default now()
        );

        create table if not exists data_quality_score_daily (
            tenant_id text not null,
            score_date date not null,
            quality_score numeric(5, 2) not null,
            passed_checks integer not null,
            failed_checks integer not null,
            warning_checks integer not null,
            critical_checks integer not null,
            updated_at timestamptz not null default now(),
            primary key (tenant_id, score_date)
        );

        create table if not exists benchmark_run_results (
            benchmark_run_id uuid primary key default gen_random_uuid(),
            benchmark_name text not null,
            tenant_id text not null,
            target_url text not null,
            total_events integer not null,
            elapsed_seconds numeric(14, 4) not null,
            events_per_second numeric(14, 4) not null,
            failure_count integer not null,
            p50_latency_ms numeric(14, 4),
            p95_latency_ms numeric(14, 4),
            p99_latency_ms numeric(14, 4),
            max_latency_ms numeric(14, 4),
            result_payload jsonb not null default '{}'::jsonb,
            created_at timestamptz not null default now()
        );

        create index if not exists idx_dlq_replay_audit_time
            on dlq_replay_audit (replayed_at desc);
        create index if not exists idx_quality_results_time
            on data_quality_check_results (checked_at desc);
        create index if not exists idx_quality_score_tenant_date
            on data_quality_score_daily (tenant_id, score_date desc);
        create index if not exists idx_benchmark_results_time
            on benchmark_run_results (created_at desc);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        drop table if exists benchmark_run_results;
        drop table if exists data_quality_score_daily;
        drop table if exists data_quality_check_results;
        drop table if exists dlq_replay_audit;
        """
    )

