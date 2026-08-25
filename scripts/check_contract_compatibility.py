from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# The comparison algorithm itself now lives in
# platform_shared.schema_compatibility, shared with the runtime Schema
# Registry service (services/schema-registry-service) added in the Kafka
# Schema Registry implementation, so the offline governance check here and the
# live registry's compatibility endpoint can never drift apart.
_SHARED_PATH = Path(__file__).resolve().parent.parent / "services" / "shared"
if str(_SHARED_PATH) not in sys.path:
    sys.path.insert(0, str(_SHARED_PATH))
from platform_shared.schema_compatibility import compare_backward_compatible  # noqa: E402


def load_schema(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def validate_cases(cases_path: Path) -> list[str]:
    cases = json.loads(cases_path.read_text())["cases"]
    errors: list[str] = []
    root = cases_path.parents[1]
    for case in cases:
        old_schema = load_schema(root / case["old_schema"])
        new_schema = load_schema(root / case["new_schema"])
        case_errors = compare_backward_compatible(old_schema, new_schema)
        expected = case.get("expected", "compatible")
        if expected == "compatible" and case_errors:
            errors.append(f"{case['name']} expected compatible but failed: {case_errors}")
        if expected == "breaking" and not case_errors:
            errors.append(f"{case['name']} expected breaking but passed")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description="Check JSON Schema backward compatibility cases.")
    parser.add_argument("--cases", default="contracts/compatibility_tests/order_v1_to_v2.json")
    args = parser.parse_args()
    errors = validate_cases(Path(args.cases))
    if errors:
        print("\n".join(errors), file=sys.stderr)
        raise SystemExit(1)
    print("contract compatibility checks ok")


if __name__ == "__main__":
    main()
