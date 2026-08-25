# P6 2.0 Before -> After

The “before” column reflects the repository's prior evidence boundary. The
“after” column reflects the current bounded local validation; none is production
or cloud evidence.

| Capability | Before | After | Evidence |
| --- | --- | --- | --- |
| Kafka path | logic implemented/tested; latest runtime environment-limited | actual API -> Kafka -> consumer -> PostgreSQL -> analytics result | `evidence/runtime/kafka/end_to_end.md` |
| Delivery semantics | manual commits and inbox pattern | offset reset caused redelivery; inbox held one row and duplicate DB effects were zero | `evidence/runtime/kafka/delivery_semantics.json` |
| Schema registry | project-native implementation and tests | service loaded five subjects; compatible changes passed and breaking changes were rejected | `evidence/runtime/schema_registry/result.json` |
| PostgreSQL RLS | implemented and previously verified | PostgreSQL 16 live test across 11 FORCE RLS tables, including fail-closed and bypass paths | `evidence/runtime/postgres_rls/result.md` |
| Redis | caching/fallback implemented | two-tenant miss/hit keys and live PostgreSQL fallback/recovery verified | `evidence/runtime/redis/result.md` |
| Spark streaming | five queries implemented/tested; long-running launch limited | five real queries consumed Kafka, classified event time, wrote windows, and resumed checkpoint state | `evidence/runtime/streaming/RESULT.md` |
| dbt | prior local proof | current parse/build: 17 pass, 0 warn/error/skip | `evidence/runtime/dbt/result.md` |
| Reconciliation | implemented | nine metric checks and 14 Kafka/dbt rows reconciled without critical mismatch | `evidence/runtime/reconciliation/result.md` |
| Airflow | implemented, latest audit not run | two DAGs imported; controlled defects corrected; 12-task operational and 10-task batch DAGs passed | `evidence/runtime/airflow/result.md` |
| Prometheus/Grafana | implementation/configuration | seven targets up, 19 rules loaded, 33/33 panel expressions valid | `evidence/runtime/prometheus/`, `evidence/runtime/grafana/` |
| OpenTelemetry | implemented/tested | local Jaeger captured one seven-span, two-service trace with W3C context continued across Kafka | `evidence/runtime/opentelemetry/TRACE_VALIDATION.md` |
| Kubernetes | configuration and historical 8/8 evidence | fresh disposable kind deployment reached 8/8; pod replacement took 3 seconds | `evidence/runtime/kubernetes/result.md` |
| Helm | lint/render only | local install, upgrade, rollback, RLS init, and Kafka topic hook verified | `evidence/runtime/helm/result.md` |
| KEDA | ScaledObject/configuration evidence | KEDA 2.20.2 scaled processing 1 -> 5 -> 1 as lag rose to 17K then drained | `evidence/runtime/keda/` |
| Performance | limited/sample artifacts | deterministic 1K, 10K, three-tenant 1.5K, and 20K profiles | `evidence/runtime/performance/` |
| Terraform/AWS | validated architecture, never deployed | unchanged by design; init without backend, fmt, validate only | `docs/cloud-deployment.md` |
