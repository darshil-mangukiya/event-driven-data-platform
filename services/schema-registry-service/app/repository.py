"""PostgreSQL-backed storage for the runtime Schema Registry.

Deliberately small: this is not a general-purpose registry — it enforces
exactly the compatibility contract this platform's own event pipeline
needs (one active compatibility mode per subject, immutable version
history, an auditable compatibility-check log), backed by the same
PostgreSQL the rest of the platform already runs on rather than adding a
new storage system.
"""

from __future__ import annotations

import json
from typing import Any

from platform_shared.database import Postgres


class SchemaRegistryRepository:
    def __init__(self, postgres: Postgres) -> None:
        self.postgres = postgres

    async def list_subjects(self) -> list[str]:
        rows = await self.postgres.fetch("select subject from schema_registry_subjects order by subject")
        return [row["subject"] for row in rows]

    async def get_subject(self, subject: str) -> dict[str, Any] | None:
        rows = await self.postgres.fetch(
            "select subject, compatibility_mode, created_at from schema_registry_subjects where subject = $1",
            subject,
        )
        return dict(rows[0]) if rows else None

    async def ensure_subject(self, subject: str, compatibility_mode: str) -> None:
        await self.postgres.execute(
            """
            insert into schema_registry_subjects (subject, compatibility_mode)
            values ($1, $2)
            on conflict (subject) do nothing
            """,
            subject,
            compatibility_mode,
        )

    async def latest_version(self, subject: str) -> dict[str, Any] | None:
        rows = await self.postgres.fetch(
            """
            select subject, version, schema_id, schema_json, registered_at, registered_by
            from schema_registry_versions
            where subject = $1
            order by version desc
            limit 1
            """,
            subject,
        )
        if not rows:
            return None
        row = dict(rows[0])
        row["schema_json"] = json.loads(row["schema_json"]) if isinstance(row["schema_json"], str) else row["schema_json"]
        return row

    async def get_version(self, subject: str, version: int) -> dict[str, Any] | None:
        rows = await self.postgres.fetch(
            """
            select subject, version, schema_id, schema_json, registered_at, registered_by
            from schema_registry_versions
            where subject = $1 and version = $2
            """,
            subject,
            version,
        )
        if not rows:
            return None
        row = dict(rows[0])
        row["schema_json"] = json.loads(row["schema_json"]) if isinstance(row["schema_json"], str) else row["schema_json"]
        return row

    async def list_versions(self, subject: str) -> list[int]:
        rows = await self.postgres.fetch(
            "select version from schema_registry_versions where subject = $1 order by version",
            subject,
        )
        return [row["version"] for row in rows]

    async def register_version(
        self, subject: str, version: int, schema_json: dict[str, Any], registered_by: str | None
    ) -> dict[str, Any]:
        rows = await self.postgres.fetch(
            """
            insert into schema_registry_versions (subject, version, schema_json, registered_by)
            values ($1, $2, $3::jsonb, $4)
            returning subject, version, schema_id, schema_json, registered_at, registered_by
            """,
            subject,
            version,
            json.dumps(schema_json),
            registered_by,
        )
        row = dict(rows[0])
        row["schema_json"] = json.loads(row["schema_json"]) if isinstance(row["schema_json"], str) else row["schema_json"]
        return row

    async def record_compatibility_check(
        self,
        subject: str,
        compatibility_mode: str,
        is_compatible: bool,
        errors: list[str],
        dry_run: bool,
    ) -> None:
        await self.postgres.execute(
            """
            insert into schema_registry_compatibility_checks
                (subject, compatibility_mode, is_compatible, errors, dry_run)
            values ($1, $2, $3, $4::jsonb, $5)
            """,
            subject,
            compatibility_mode,
            is_compatible,
            json.dumps(errors),
            dry_run,
        )

    async def recent_compatibility_checks(self, subject: str, limit: int = 20) -> list[dict[str, Any]]:
        rows = await self.postgres.fetch(
            """
            select subject, compatibility_mode, is_compatible, errors, dry_run, checked_at
            from schema_registry_compatibility_checks
            where subject = $1
            order by checked_at desc
            limit $2
            """,
            subject,
            limit,
        )
        results = []
        for row in rows:
            row = dict(row)
            row["errors"] = json.loads(row["errors"]) if isinstance(row["errors"], str) else row["errors"]
            results.append(row)
        return results
