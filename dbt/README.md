# dbt-Style Analytics Layer

This folder adds the semantic modeling layer that an Analytics Engineer would expect on top of the platform serving database.

Included:

- Staging models for orders, payments, and user sessions.
- Mart models for tenant daily metrics and product performance.
- Source and model tests for important keys and metric ranges.
- Runnable semantic model and metric resources in `models/semantic_metrics.yml`.
- Non-runnable historical metric definition examples in `examples/metric_definitions.yml`.

Local command shape:

```bash
cd dbt
python -m pip install -r requirements-dbt.txt
dbt deps
dbt parse --profiles-dir .
dbt build --profiles-dir .
```

`dbt parse` validates project syntax without a live database connection. `dbt build` requires the local PostgreSQL service to be running and loaded with the platform schema.

If port `5432` is already used by another local PostgreSQL service, start Compose with `POSTGRES_HOST_PORT=55432` and run dbt with `POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=55432`.

dbt build was verified against the local Docker PostgreSQL runtime using the documented Compose configuration. Verified dbt scope: 7 models and 10 data tests passing through `dbt build`.

The repo does not require dbt to run the FastAPI services. This layer is for downstream semantic modeling, governed metric definitions, and BI-ready PostgreSQL/API outputs.
