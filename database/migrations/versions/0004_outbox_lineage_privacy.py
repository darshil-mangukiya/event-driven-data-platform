from __future__ import annotations

from alembic import op

revision = "0004_outbox_lineage_privacy"
down_revision = "0003_reconciliation_audit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        create table if not exists event_outbox (
            outbox_id uuid primary key default gen_random_uuid(),
            aggregate_id text not null,
            tenant_id text not null references tenant_config(tenant_id),
            event_id text not null unique,
            event_type text not null,
            source_service text not null,
            payload_version integer not null default 1,
            payload jsonb not null,
            idempotency_key text,
            status text not null default 'pending',
            attempts integer not null default 0,
            available_at timestamptz not null default now(),
            locked_at timestamptz,
            published_at timestamptz,
            error_message text,
            created_at timestamptz not null default now(),
            updated_at timestamptz not null default now()
        );

        create table if not exists event_inbox (
            consumer_name text not null,
            event_id text not null,
            tenant_id text not null references tenant_config(tenant_id),
            event_type text not null,
            trace_id text not null,
            status text not null default 'processing',
            received_at timestamptz not null default now(),
            processed_at timestamptz,
            error_message text,
            primary key (consumer_name, event_id)
        );

        create table if not exists lineage_events (
            lineage_event_id uuid primary key default gen_random_uuid(),
            event_type text not null,
            job_name text not null,
            run_id text not null,
            tenant_id text,
            input_datasets jsonb not null default '[]'::jsonb,
            output_datasets jsonb not null default '[]'::jsonb,
            status text not null,
            event_timestamp timestamptz not null default now(),
            metadata jsonb not null default '{}'::jsonb
        );

        create table if not exists privacy_erasure_requests (
            request_id uuid primary key default gen_random_uuid(),
            tenant_id text not null references tenant_config(tenant_id),
            subject_id text not null,
            subject_type text not null default 'user',
            status text not null default 'requested',
            requested_by text not null,
            reason text,
            requested_at timestamptz not null default now(),
            completed_at timestamptz,
            evidence jsonb not null default '{}'::jsonb
        );

        create table if not exists data_retention_policies (
            policy_id uuid primary key default gen_random_uuid(),
            table_name text not null,
            tenant_plan text not null default 'all',
            retention_days integer not null,
            action text not null default 'archive_then_delete',
            is_active boolean not null default true,
            updated_at timestamptz not null default now(),
            unique (table_name, tenant_plan)
        );

        insert into data_retention_policies (table_name, tenant_plan, retention_days, action)
        values
            ('raw_events', 'all', 180, 'archive_then_delete'),
            ('api_usage_log', 'all', 90, 'delete'),
            ('service_health_metrics', 'all', 90, 'archive_then_delete'),
            ('data_quality_check_results', 'all', 365, 'archive_then_delete')
        on conflict (table_name, tenant_plan) do nothing;

        create index if not exists idx_event_outbox_status_available
            on event_outbox (status, available_at, created_at);
        create index if not exists idx_event_outbox_tenant
            on event_outbox (tenant_id, created_at desc);
        create index if not exists idx_event_inbox_tenant_status
            on event_inbox (tenant_id, status, received_at desc);
        create index if not exists idx_lineage_events_job_time
            on lineage_events (job_name, event_timestamp desc);
        create index if not exists idx_privacy_erasure_tenant_status
            on privacy_erasure_requests (tenant_id, status, requested_at desc);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        drop table if exists data_retention_policies;
        drop table if exists privacy_erasure_requests;
        drop table if exists lineage_events;
        drop table if exists event_inbox;
        drop table if exists event_outbox;
        """
    )
