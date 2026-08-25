from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from cloudscale_airflow_utils import (
    END_DATE,
    FROM_DATE,
    SPARK_SUBMIT,
    SPARK_WINDOW_DAYS,
    TENANT_ID,
    project_command,
)

DAG_ID = "cloudscale_batch_jobs"

default_args = {
    "owner": "data-platform",
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    description="Local batch orchestration for Spark, backfill, and reconciliation jobs.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cloudscale", "batch", "local"],
) as dag:
    start = EmptyOperator(task_id="start")

    validate_event_contracts = BashOperator(
        task_id="validate_event_contracts",
        bash_command=project_command(
            "python scripts/validate_event_contracts.py",
            pythonpath="services/shared",
        ),
    )

    check_contract_compatibility = BashOperator(
        task_id="check_contract_compatibility",
        bash_command=project_command(
            "python scripts/check_contract_compatibility.py",
            pythonpath="services/shared",
        ),
    )

    event_normalization = BashOperator(
        task_id="event_normalization",
        bash_command=project_command(
            f"{SPARK_SUBMIT} spark/jobs/event_normalization_job.py --days {SPARK_WINDOW_DAYS}"
        ),
    )

    revenue_aggregation = BashOperator(
        task_id="revenue_aggregation",
        bash_command=project_command(
            f"{SPARK_SUBMIT} spark/jobs/batch_revenue_aggregates.py --days {SPARK_WINDOW_DAYS}"
        ),
    )

    sessionization = BashOperator(
        task_id="sessionization",
        bash_command=project_command(
            f"{SPARK_SUBMIT} spark/jobs/sessionization_job.py --days {SPARK_WINDOW_DAYS}"
        ),
    )

    lakehouse_compaction = BashOperator(
        task_id="lakehouse_compaction",
        bash_command=project_command(
            f"{SPARK_SUBMIT} spark/jobs/lakehouse_compaction.py "
            "--input /tmp/spark/normalized-events "
            "--output /tmp/spark/compacted-events"
        ),
    )

    metric_backfill_dry_run = BashOperator(
        task_id="metric_backfill_dry_run",
        bash_command=project_command(
            "python scripts/backfill_metrics.py "
            f"--tenant-id {TENANT_ID} "
            f"--start-date {FROM_DATE} "
            f"--end-date {END_DATE} "
            "--dry-run --pretty",
            pythonpath="services/shared",
        ),
    )

    reconciliation_dry_run = BashOperator(
        task_id="reconciliation_dry_run",
        bash_command=project_command(
            "python scripts/reconcile_metrics.py "
            f"--tenant-id {TENANT_ID} "
            f"--start-date {FROM_DATE} "
            f"--end-date {END_DATE} "
            "--dry-run --pretty",
            pythonpath="services/shared",
        ),
    )

    finish = EmptyOperator(task_id="finish")

    start >> validate_event_contracts >> check_contract_compatibility
    check_contract_compatibility >> [
        event_normalization,
        revenue_aggregation,
        sessionization,
    ]
    event_normalization >> lakehouse_compaction
    [
        revenue_aggregation,
        sessionization,
        lakehouse_compaction,
    ] >> metric_backfill_dry_run
    metric_backfill_dry_run >> reconciliation_dry_run >> finish
