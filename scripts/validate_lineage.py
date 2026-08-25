"""Validate the data lineage graph: cycle detection, orphan-table
detection, and cross-reference validation of every "table feeds/is-fed-by
external system X" claim against the actual code for that external system.

Usage:
    PYTHONPATH=. python scripts/validate_lineage.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lineage.graph import validate_graph  # noqa: E402


def main() -> None:
    results = validate_graph()
    total_errors = sum(len(v) for v in results.values())
    for category, items in results.items():
        status = "ok" if not items else f"{len(items)} issue(s)"
        print(f"[{status}] {category}")
        for item in items:
            print(f"  - {item}")

    if total_errors:
        print(f"\nlineage validation FAILED: {total_errors} total issue(s)", file=sys.stderr)
        raise SystemExit(1)
    print("\nlineage graph ok")


if __name__ == "__main__":
    main()
