"""Generate the data-product catalog and requirements-traceability reports
from contracts/data_products/*.yml.

Usage:
    PYTHONPATH=. python scripts/generate_data_product_catalog.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_products.generator import write_reports  # noqa: E402


def main() -> None:
    paths = write_reports()
    for name, path in paths.items():
        print(f"generated {name}: {path}")


if __name__ == "__main__":
    main()
