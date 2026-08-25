# Optional Local Airflow Orchestration

The platform keeps Kafka streaming consumers service-driven. Airflow is an optional local scheduler for finite operational and batch workflows such as validation, reconciliation, metric backfill dry-runs, and Spark batch jobs.

## DAGs

| DAG | Purpose |
| --- | --- |
| `cloudscale_operational_checks` | Runs contract checks, catalog and metric validation, tenant RLS validation, privacy catalog validation, schema drift report, sample artifact validation, reconciliation dry-run, and validation evidence generation. |
| `cloudscale_batch_jobs` | Runs contract prechecks, finite PySpark batch jobs, metric backfill dry-run, and reconciliation dry-run. |

The batch DAG does not schedule `spark/jobs/streaming_enrichment.py` because that script is a long-running Kafka streaming job. Kafka consumers and streaming services remain outside Airflow.

## Start Airflow Locally

Start the core platform and optional Airflow services:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up --build
```

Airflow web UI:

```text
http://localhost:8088
```

Default local login:

```text
username: admin
password: admin
```

## Useful Commands

List DAGs after the Airflow services are running:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-webserver airflow dags list
```

Run a DAG once for local validation:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-webserver airflow dags test cloudscale_operational_checks 2026-06-01
```

Validate DAG files without starting Airflow:

```bash
python -m compileall -q airflow
PYTHONPATH=.:services/shared:.test-deps python -m pytest tests/test_airflow_dags.py -q
```

## Local Scope

This setup uses a local Airflow metadata database and a local executor. It is designed for development and repository validation, not production scheduling. A production scheduler would need hardened secrets, remote logs, executor sizing, alert routing, access control, and deployment automation.
