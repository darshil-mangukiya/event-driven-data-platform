from __future__ import annotations

from alembic import op

revision = "0007_schema_registry"
down_revision = "0006_streaming_serving_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        -- Runtime Schema Registry: one row per registered subject
        -- (mirrors Confluent Schema Registry's "subject" concept — one
        -- subject per Kafka topic/event family, e.g. "order-events").
        create table if not exists schema_registry_subjects (
            subject text primary key,
            compatibility_mode text not null default 'BACKWARD',
            created_at timestamptz not null default now()
        );

        -- One row per registered schema version for a subject. Versions
        -- are immutable once written — a new version is only ever
        -- appended after passing the compatibility check against the
        -- current latest version for that subject.
        create table if not exists schema_registry_versions (
            subject text not null references schema_registry_subjects(subject),
            version integer not null,
            schema_id uuid not null default gen_random_uuid(),
            schema_json jsonb not null,
            registered_at timestamptz not null default now(),
            registered_by text,
            primary key (subject, version)
        );

        create index if not exists idx_schema_registry_versions_subject
            on schema_registry_versions (subject, version desc);

        -- Audit trail of every compatibility check the registry has
        -- evaluated (both real registrations and dry-run /compatibility
        -- calls) — real, queryable evidence that the enforcement layer
        -- ran, rather than only recording that the implementation exists.
        create table if not exists schema_registry_compatibility_checks (
            id uuid primary key default gen_random_uuid(),
            subject text not null,
            compatibility_mode text not null,
            is_compatible boolean not null,
            errors jsonb not null default '[]'::jsonb,
            dry_run boolean not null default true,
            checked_at timestamptz not null default now()
        );

        create index if not exists idx_schema_registry_compat_checks_subject
            on schema_registry_compatibility_checks (subject, checked_at desc);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        drop index if exists idx_schema_registry_compat_checks_subject;
        drop table if exists schema_registry_compatibility_checks;

        drop index if exists idx_schema_registry_versions_subject;
        drop table if exists schema_registry_versions;

        drop table if exists schema_registry_subjects;
        """
    )
