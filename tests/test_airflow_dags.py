from __future__ import annotations

import ast
from pathlib import Path

DAG_DIR = Path("airflow/dags")
OPERATIONAL_DAG = DAG_DIR / "cloudscale_operational_checks_dag.py"
BATCH_DAG = DAG_DIR / "cloudscale_batch_jobs_dag.py"


def parse_python(path: Path) -> ast.Module:
    source = path.read_text()
    compile(source, str(path), "exec")
    return ast.parse(source)


def dag_id_from(module: ast.Module) -> str | None:
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "DAG_ID":
                    if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                        return node.value.value
    return None


def task_ids_from(module: ast.Module) -> list[str]:
    task_ids: list[str] = []
    for node in ast.walk(module):
        if isinstance(node, ast.keyword) and node.arg == "task_id":
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                task_ids.append(node.value.value)
    return task_ids


def test_airflow_dag_files_exist_and_compile() -> None:
    assert OPERATIONAL_DAG.exists()
    assert BATCH_DAG.exists()
    assert (DAG_DIR / "cloudscale_airflow_utils.py").exists()
    parse_python(OPERATIONAL_DAG)
    parse_python(BATCH_DAG)


def test_project_commands_keep_repository_root_on_pythonpath() -> None:
    source = (DAG_DIR / "cloudscale_airflow_utils.py").read_text()
    assert 'import_paths = "."' in source
    assert 'f".:{pythonpath}"' in source


def test_operational_checks_dag_structure() -> None:
    module = parse_python(OPERATIONAL_DAG)
    assert dag_id_from(module) == "cloudscale_operational_checks"
    task_ids = task_ids_from(module)
    assert len(task_ids) == len(set(task_ids))
    assert {
        "validate_event_contracts",
        "check_contract_compatibility",
        "validate_catalog",
        "validate_metric_contracts",
        "validate_tenant_rls",
        "validate_privacy_catalog",
        "schema_drift_report",
        "validate_sample_artifacts",
        "reconciliation_dry_run",
        "generate_validation_evidence",
    }.issubset(task_ids)

    source = OPERATIONAL_DAG.read_text()
    assert "validate_event_contracts >> check_contract_compatibility" in source
    assert "reconciliation_dry_run >> generate_validation_evidence" in source


def test_batch_jobs_dag_structure() -> None:
    module = parse_python(BATCH_DAG)
    assert dag_id_from(module) == "cloudscale_batch_jobs"
    task_ids = task_ids_from(module)
    assert len(task_ids) == len(set(task_ids))
    assert {
        "validate_event_contracts",
        "check_contract_compatibility",
        "event_normalization",
        "revenue_aggregation",
        "sessionization",
        "lakehouse_compaction",
        "metric_backfill_dry_run",
        "reconciliation_dry_run",
    }.issubset(task_ids)

    source = BATCH_DAG.read_text()
    assert "streaming_enrichment" not in source
    assert "event_normalization >> lakehouse_compaction" in source
    assert "--input /tmp/spark/normalized-events" in source
    assert "metric_backfill_dry_run >> reconciliation_dry_run" in source
