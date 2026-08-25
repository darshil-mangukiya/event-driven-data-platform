# P6 2.0 Technology Evidence Matrix

“Local” means bounded execution on one development machine. “Cloud” is **No**
for every row.

| Technology | Implemented | Unit tested | Integration tested | Locally executed | Cloud executed | Primary evidence | Public claim qualifier |
| --- | :---: | :---: | :---: | :---: | :---: | --- | --- |
| Python | Yes | Yes | Yes | Yes | No | `tests/`, runtime manifest | Built/tested locally |
| FastAPI | Yes | Yes | Yes | Yes | No | Kafka E2E result | Seven apps, 50 routes |
| Kafka | Yes | Yes | Yes | Yes | No | `evidence/runtime/kafka/` | Single local broker |
| Consumer groups/offsets | Yes | Yes | Yes | Yes | No | delivery semantics JSON | Manual commits; at-least-once |
| JSON Schema | Yes | Yes | Yes | Yes | No | schema registry result | 20 files, 12 event types |
| Project schema registry | Yes | Yes | Yes | Yes | No | schema registry result | Custom service, five runtime subjects |
| AsyncAPI | Yes | Yes | Yes | No (validated) | No | `contracts/asyncapi.yml` | Specification/configuration validated |
| PostgreSQL | Yes | Yes | Yes | Yes | No | RLS/dbt/reconciliation results | Local PostgreSQL 16 |
| Row-level security | Yes | Yes | Yes | Yes | No | RLS result | 11 FORCE RLS tables |
| Redis | Yes | Yes | Yes | Yes | No | Redis result | Tenant keys and fail-open read path |
| Spark Structured Streaming | Yes | Yes | Yes | Yes | No | streaming result/matrix | Five bounded local queries |
| PySpark | Yes | Yes | Yes | Yes | No | 43-test focused streaming suite | Local-mode Spark |
| dbt | Yes | Yes | Yes | Yes | No | dbt result | 7 models, 10 tests |
| Prometheus | Yes | Yes | Yes | Yes | No | Prometheus result | Seven local targets |
| Grafana | Yes | Asset tests | Yes | Yes | No | Grafana result | API/provisioning validation; no screenshot |
| OpenTelemetry | Yes | Yes | Yes | Yes | No | OTel trace validation | One local seven-span, two-service trace; no PostgreSQL span |
| Airflow | Yes | Yes | Yes | Both DAGs | No | Airflow result | 12-task operational and 10-task batch runs |
| Docker/Compose | Yes | Config tests | Yes | Yes | No | runtime manifest | Local integration environment |
| Kubernetes | Yes | Manifest tests | Yes | Yes | No | Kubernetes result | Disposable single-node kind |
| Helm | Yes | Lint/render | Yes | Yes | No | Helm result | Install/upgrade/rollback locally |
| KEDA | Yes | Config tests | Yes | Yes | No | autoscaling timeline | Local lag-driven 1 -> 5 -> 1 |
| Terraform | Yes | Validation | No | No (validated) | No | Terraform validation commands | No plan/apply |
| AWS reference architecture | Config only | Validation | No | No | No | `docs/cloud-deployment.md` | Never cloud deployed |
| Load testing | Yes | Result validation | Yes | Yes | No | performance artifacts | Synthetic batch API acceptance |
| GitHub Actions | Yes | Config validation | No | No | No | `.github/workflows/` | Workflow configured; not changed remotely |
