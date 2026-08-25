# dbt Build Verification

Date: 2026-07-12

Runtime:
- Docker PostgreSQL via Compose
- Host: 127.0.0.1
- Port: 55432
- Database: data_platform
- User: platform

Commands verified:
- dbt deps
- dbt debug
- dbt parse
- dbt build
- dbt ls --resource-type model
- dbt ls --resource-type test

Result:
- dbt build: passed
- Models: 7
- Tests: 10
- Build summary: PASS=17 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=17

Notes:
- This is local Docker/PostgreSQL verification using the development dataset.
- Seeded tenant daily metric rows were present in PostgreSQL; processed event tables may be empty unless the full ingestion and processing stack is run.
- This is not a live production deployment.
