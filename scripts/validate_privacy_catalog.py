from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REQUIRED_KEYS = {"dataset", "field", "classification", "handling", "retention_policy"}
ALLOWED_CLASSIFICATIONS = {"public", "internal", "confidential", "restricted_pii"}


def validate_privacy_catalog(catalog: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    entries = catalog.get("fields", [])
    if not entries:
        errors.append("privacy catalog has no fields")
    for index, entry in enumerate(entries):
        missing = REQUIRED_KEYS - set(entry)
        if missing:
            errors.append(f"field entry {index} missing {sorted(missing)}")
        if entry.get("classification") not in ALLOWED_CLASSIFICATIONS:
            errors.append(f"field entry {index} has invalid classification {entry.get('classification')}")
        if entry.get("classification") == "restricted_pii" and entry.get("handling") != "mask_or_hash":
            errors.append(f"restricted PII field {entry.get('dataset')}.{entry.get('field')} must use mask_or_hash")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate privacy/PII classification catalog.")
    parser.add_argument("--catalog", default="governance/pii_classification.json")
    args = parser.parse_args()
    errors = validate_privacy_catalog(json.loads(Path(args.catalog).read_text()))
    if errors:
        print("\n".join(errors), file=sys.stderr)
        raise SystemExit(1)
    print("privacy catalog ok")


if __name__ == "__main__":
    main()
