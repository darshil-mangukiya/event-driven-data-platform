from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from cloudscale_airflow_utils import END_DATE, FROM_DATE, TENANT_ID, project_command

DAG_ID = "cloudscale_operational_checks"

default_args = {
    "owner": "data-platform",
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    description="Local operational validation and evidence workflow for the data platform.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["cloudscale", "validation", "local"],
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

    validate_catalog = BashOperator(
        task_id="validate_catalog",
        bash_command=project_command("python scripts/validate_catalog.py"),
    )

    validate_metric_contracts = BashOperator(
        task_id="validate_metric_contracts",
        bash_command=project_command("python scripts/validate_metric_contracts.py"),
    )

    validate_tenant_rls = BashOperator(
        task_id="validate_tenant_rls",
        bash_command=project_command("python scripts/validate_tenant_rls.py"),
    )

    validate_privacy_catalog = BashOperator(
        task_id="validate_privacy_catalog",
        bash_command=project_command("python scripts/validate_privacy_catalog.py"),
    )

    schema_drift_report = BashOperator(
        task_id="schema_drift_report",
        bash_command=project_command("python scripts/schema_drift_report.py --pretty"),
    )

    validate_sample_artifacts = BashOperator(
        task_id="validate_sample_artifacts",
        bash_command=project_command(
            "python scripts/validate_sample_artifacts.py",
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

    generate_validation_evidence = BashOperator(
        task_id="generate_validation_evidence",
        bash_command=project_command(
            "python scripts/generate_evidence_bundle.py --output-dir evidence/validation --pretty"
        ),
    )

    finish = EmptyOperator(task_id="finish")

    start >> validate_event_contracts >> check_contract_compatibility
    start >> [
        validate_catalog,
        validate_metric_contracts,
        validate_tenant_rls,
        validate_privacy_catalog,
        schema_drift_report,
        validate_sample_artifacts,
    ]

    [
        check_contract_compatibility,
        validate_catalog,
        validate_metric_contracts,
        validate_tenant_rls,
        validate_privacy_catalog,
        schema_drift_report,
        validate_sample_artifacts,
    ] >> reconciliation_dry_run

    reconciliation_dry_run >> generate_validation_evidence >> finish
