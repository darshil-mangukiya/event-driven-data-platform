from __future__ import annotations

import os
from shlex import quote

PROJECT_ROOT = os.getenv("CLOUDSCALE_PROJECT_ROOT", "/opt/airflow/project")
TENANT_ID = "${CLOUDSCALE_TENANT_ID:-tenant_demo}"
FROM_DATE = "${CLOUDSCALE_BACKFILL_FROM_DATE:-2026-05-01}"
END_DATE = "${CLOUDSCALE_BACKFILL_END_DATE:-2026-05-07}"
SPARK_WINDOW_DAYS = "${CLOUDSCALE_SPARK_WINDOW_DAYS:-7}"
SPARK_SUBMIT = os.getenv(
    "CLOUDSCALE_SPARK_SUBMIT",
    "spark-submit --packages org.postgresql:postgresql:42.7.3",
)


def project_command(command: str, *, pythonpath: str | None = None) -> str:
    # Airflow invokes project scripts by filename, which places ``scripts/``
    # (not the repository root) on sys.path. Keep the project root available
    # for native packages such as ``lineage`` and add shared service modules
    # when a task needs them.
    import_paths = "." if pythonpath is None else f".:{pythonpath}"
    env_prefix = f"PYTHONPATH={quote(import_paths)} "
    return "\n".join(
        [
            "set -euo pipefail",
            f"cd {quote(PROJECT_ROOT)}",
            f"{env_prefix}{command}",
        ]
    )
