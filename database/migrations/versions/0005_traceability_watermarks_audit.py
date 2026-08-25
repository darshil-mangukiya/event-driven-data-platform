from __future__ import annotations

from alembic import op

revision = "0005_traceability_watermarks_audit"
down_revision = "0004_outbox_lineage_privacy"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        alter table raw_events add column if not exists correlation_id text;
        alter table raw_events add column if not exists causation_id text;
        alter table raw_events add column if not exists idempotency_key text;

        create index if not exists idx_raw_events_traceability
            on raw_events (tenant_id, correlation_id, event_timestamp desc);
        create index if not exists idx_raw_events_idempotency
            on raw_events (tenant_id, idempotency_key);

        create table if not exists pipeline_watermarks (
            pipeline_name text not null,
            tenant_id text not null references tenant_config(tenant_id),
            source_topic text not null,
            last_processed_timestamp timestamptz,
            last_processed_offset bigint,
            status text not null default 'active',
            updated_at timestamptz not null default now(),
            metadata jsonb not null default '{}'::jsonb,
            primary key (pipeline_name, tenant_id, source_topic)
        );

        create index if not exists idx_pipeline_watermarks_status
            on pipeline_watermarks (status, updated_at desc);

        alter table api_usage_log add column if not exists role text;
        alter table api_usage_log add column if not exists trace_id text;

        create index if not exists idx_api_usage_log_tenant_time
            on api_usage_log (tenant_id, requested_at desc);
        create index if not exists idx_api_usage_log_trace
            on api_usage_log (trace_id);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        drop index if exists idx_api_usage_log_trace;
        drop index if exists idx_api_usage_log_tenant_time;
        alter table api_usage_log drop column if exists trace_id;
        alter table api_usage_log drop column if exists role;

        drop index if exists idx_pipeline_watermarks_status;
        drop table if exists pipeline_watermarks;

        drop index if exists idx_raw_events_idempotency;
        drop index if exists idx_raw_events_traceability;
        alter table raw_events drop column if exists idempotency_key;
        alter table raw_events drop column if exists causation_id;
        alter table raw_events drop column if exists correlation_id;
        """
    )
