"""Generate the data lineage graph report from catalog/data_catalog.json.

Usage:
    PYTHONPATH=. python scripts/generate_lineage_report.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lineage.generator import write_lineage_report  # noqa: E402


def main() -> None:
    path = write_lineage_report()
    print(f"generated lineage report: {path}")


if __name__ == "__main__":
    main()
