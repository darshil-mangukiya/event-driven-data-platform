from __future__ import annotations

from alembic import op

revision = "0003_reconciliation_audit"
down_revision = "0002_ops_quality_benchmark_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        create table if not exists reconciliation_audit (
            reconciliation_id uuid primary key default gen_random_uuid(),
            tenant_id text not null references tenant_config(tenant_id),
            metric_date date not null,
            check_name text not null,
            status text not null,
            revenue_delta numeric(18, 4) not null default 0,
            order_count_delta integer not null default 0,
            units_sold_delta integer not null default 0,
            details jsonb not null default '{}'::jsonb,
            checked_by text not null default 'local-operator',
            checked_at timestamptz not null default now()
        );

        create index if not exists idx_reconciliation_audit_tenant_date
            on reconciliation_audit (tenant_id, metric_date desc);
        """
    )


def downgrade() -> None:
    op.execute("drop table if exists reconciliation_audit;")
