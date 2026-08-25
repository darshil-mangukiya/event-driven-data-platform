"""Validate the data-product registry, modeled consumer catalog, and
requirements traceability matrix, including cross-reference checks against
the real implemented system (API routes, event contracts, data catalog,
metric contracts, SLO catalog).

Usage:
    PYTHONPATH=.:services/shared python scripts/validate_data_products.py
    PYTHONPATH=.:services/shared python scripts/validate_data_products.py --no-live-routes --no-live-tests
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_products.validator import validate_all  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate data product contracts.")
    parser.add_argument("--no-live-routes", action="store_true", help="Skip importing analytics-service to check live API routes.")
    parser.add_argument("--no-live-tests", action="store_true", help="Skip collecting referenced pytest node ids.")
    args = parser.parse_args()

    results = validate_all(
        check_live_routes=not args.no_live_routes,
        check_live_tests=not args.no_live_tests,
    )
    total_errors = sum(len(v) for v in results.values())
    for category, errors in results.items():
        status = "ok" if not errors else f"{len(errors)} error(s)"
        print(f"[{status}] {category}")
        for error in errors:
            print(f"  - {error}")

    if total_errors:
        print(f"\ndata product validation FAILED: {total_errors} total error(s)", file=sys.stderr)
        raise SystemExit(1)
    print("\ndata product contracts ok")


if __name__ == "__main__":
    main()
