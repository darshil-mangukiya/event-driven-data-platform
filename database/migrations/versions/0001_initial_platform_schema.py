from __future__ import annotations

from pathlib import Path

from alembic import op

revision = "0001_initial_platform_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema_path = Path(__file__).resolve().parents[2] / "init" / "001_schema.sql"
    op.execute(schema_path.read_text())


def downgrade() -> None:
    op.execute(
        """
        drop view if exists tenant_analytics_isolated;
        drop table if exists api_usage_log;
        drop table if exists pipeline_run_log;
        drop table if exists service_health_metrics;
        drop table if exists alerts;
        drop table if exists fraud_or_risk_events;
        drop table if exists tenant_metrics_daily;
        drop table if exists tenant_metrics_hourly;
        drop table if exists processed_user_sessions;
        drop table if exists processed_payments;
        drop table if exists processed_orders;
        drop table if exists raw_events;
        drop table if exists tenant_products;
        drop table if exists tenant_users;
        drop table if exists tenant_config;
        """
    )

