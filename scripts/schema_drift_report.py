from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

CREATE_PATTERN = re.compile(r"create\s+(?:table|or\s+replace\s+view)\s+(?:if\s+not\s+exists\s+)?([a-zA-Z_][\w]*)", re.IGNORECASE)


def database_objects(sql: str) -> set[str]:
    return {match.group(1) for match in CREATE_PATTERN.finditer(sql)}


def migration_revisions(migrations_dir: Path) -> list[str]:
    revisions: list[str] = []
    for path in sorted((migrations_dir / "versions").glob("*.py")):
        text = path.read_text()
        match = re.search(r'revision\s*=\s*"([^"]+)"', text)
        if match:
            revisions.append(match.group(1))
    return revisions


def drift_report(schema_sql: str, catalog: dict[str, Any], migrations_dir: Path) -> dict[str, Any]:
    objects = database_objects(schema_sql)
    catalog_tables = {table["name"] for table in catalog.get("tables", [])}
    unmanaged_catalog_tables = sorted(catalog_tables - objects)
    uncataloged_core_objects = sorted(
        name
        for name in objects - catalog_tables
        if not name.startswith("tenant_analytics")
        and name
        not in {
            "tenant_config",
            "tenant_users",
            "tenant_products",
            "data_retention_policies",
        }
    )
    revisions = migration_revisions(migrations_dir)
    return {
        "status": "failed" if unmanaged_catalog_tables else "passed",
        "database_object_count": len(objects),
        "catalog_table_count": len(catalog_tables),
        "unmanaged_catalog_tables": unmanaged_catalog_tables,
        "uncataloged_core_objects": uncataloged_core_objects,
        "migration_revisions": revisions,
        "latest_revision": revisions[-1] if revisions else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare database init schema, catalog, and migrations.")
    parser.add_argument("--schema", default="database/init/001_schema.sql")
    parser.add_argument("--catalog", default="catalog/data_catalog.json")
    parser.add_argument("--migrations-dir", default="database/migrations")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    report = drift_report(
        Path(args.schema).read_text(),
        json.loads(Path(args.catalog).read_text()),
        Path(args.migrations_dir),
    )
    print(json.dumps(report, indent=2 if args.pretty else None, sort_keys=True))
    if report["status"] != "passed":
        print("schema drift detected", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
