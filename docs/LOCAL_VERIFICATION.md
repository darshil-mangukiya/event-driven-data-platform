# Local Verification

This guide describes practical runtime checks for a local platform run. It is intended for engineering verification only.

## Compose Checks

Validate the core Compose file:

```bash
docker compose config
```

Validate the optional Airflow overlay:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml config
```

## Runtime Smoke Checks

Start the core stack on a Docker-enabled machine:

```bash
docker compose -f docker-compose.yml up --build
```

Run service health and OpenAPI checks:

```bash
python scripts/docker_smoke_check.py
```

When Airflow is running:

```bash
python scripts/docker_smoke_check.py --include-airflow
```

Prometheus can be included when required:

```bash
python scripts/docker_smoke_check.py --include-prometheus
```

## Service Pages

Verify these local pages load after startup:

| Surface | URL |
| --- | --- |
| Ingestion OpenAPI | `http://localhost:8001/docs` |
| Analytics OpenAPI | `http://localhost:8003/docs` |
| Metadata OpenAPI | `http://localhost:8004/docs` |
| Demo dashboard | `http://localhost:8005/?tenant_id=tenant_demo` |
| Ops console | `http://localhost:8006/?tenant_id=tenant_demo` |
| Prometheus | `http://localhost:9090` |
| Airflow, optional | `http://localhost:8088` |

Grafana is not started by the default Compose stack. Import `monitoring/grafana_dashboard.json` into a Grafana instance if you want to inspect the dashboard configuration.

## Airflow DAG Checks

List DAGs:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-webserver airflow dags list
```

Run the operational checks DAG once:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-webserver airflow dags test cloudscale_operational_checks 2026-06-01
```

Airflow schedules finite validation, reconciliation, backfill dry-run, evidence, and Spark batch workflows. Kafka streaming remains service-driven.

## Validation Scripts

Run repository checks without requiring Docker:

```bash
PYTHONPATH=services/shared python scripts/validate_event_contracts.py
PYTHONPATH=services/shared python scripts/check_contract_compatibility.py
python scripts/validate_catalog.py
python scripts/validate_metric_contracts.py
python scripts/validate_tenant_rls.py
python scripts/validate_privacy_catalog.py
python scripts/schema_drift_report.py --pretty
PYTHONPATH=services/shared python scripts/validate_sample_artifacts.py
```

## Runtime Evidence

Useful local evidence includes:

- smoke-check output from `scripts/docker_smoke_check.py`
- loaded OpenAPI pages
- `GET /health` responses for service health
- `GET /metrics/revenue`, `GET /metrics/payment_success`, and `GET /metrics/event_throughput` responses
- Airflow DAG list and DAG test output when Airflow is included
- Prometheus target health and mounted rule files
- sample API response files under `api/fixtures/`
- generated validation files under `evidence/validation/`

The repository should not claim Docker runtime validation unless these checks pass on a Docker-enabled machine.
