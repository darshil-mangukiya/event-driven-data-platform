from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def validate_catalog(catalog: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    domains = set(catalog.get("domains", {}))
    tables = {table["name"]: table for table in catalog.get("tables", [])}

    for table in catalog.get("tables", []):
        if table.get("domain") not in domains:
            errors.append(f"table {table['name']} has unknown domain {table.get('domain')}")
        for field in ("owner", "layer", "grain", "primary_keys"):
            if not table.get(field):
                errors.append(f"table {table['name']} missing {field}")
        for downstream in table.get("downstream", []):
            if "." not in downstream and downstream not in tables:
                errors.append(f"table {table['name']} references unknown downstream {downstream}")

    for metric in catalog.get("metrics", []):
        source_table = metric.get("source_table")
        if source_table not in tables:
            errors.append(f"metric {metric['name']} references unknown source_table {source_table}")
        if not metric.get("owner"):
            errors.append(f"metric {metric['name']} missing owner")
        if not metric.get("definition"):
            errors.append(f"metric {metric['name']} missing definition")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate data catalog ownership and lineage references.")
    parser.add_argument("--catalog", default="catalog/data_catalog.json")
    args = parser.parse_args()
    catalog = json.loads(Path(args.catalog).read_text())
    errors = validate_catalog(catalog)
    if errors:
        for error in errors:
            print(f"ERROR {error}")
        raise SystemExit(1)
    print("data catalog ok")


if __name__ == "__main__":
    main()

