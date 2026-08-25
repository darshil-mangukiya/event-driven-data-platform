# Local Runtime Evidence

This directory records bounded execution performed on 2026-09-02/03 from base
source commit `fbb464d72049913b1b67a72af410746f3d90bc3c` plus the documented
runtime-enablement changes. The environment was a 14-CPU Apple Silicon host,
Docker Desktop, Docker Compose, and a disposable `kind` cluster.

These records prove local behavior; they are not production, customer, cloud,
or globally exactly-once evidence. Commands in the manifest are reproducible
and intentionally use repository-relative paths. Raw credentials, tokens,
kubeconfig, payload dumps, and large logs are excluded.

| Area | Result | Evidence |
| --- | --- | --- |
| Kafka path | API -> Kafka -> consumer -> PostgreSQL -> analytics passed | `kafka/` |
| Delivery semantics | redelivery occurred; duplicate database effects = 0 | `kafka/delivery_semantics.json` |
| Schema governance | five subjects loaded; compatible changes accepted; breaking changes rejected | `schema_registry/` |
| PostgreSQL RLS | tenant isolation and fail-closed behavior passed | `postgres_rls/` |
| Redis | tenant-scoped miss/hit and PostgreSQL fallback passed | `redis/` |
| Spark streaming | five queries, event time, late data, dedupe, checkpoint restart passed | `streaming/` |
| dbt/reconciliation | 17/17 nodes and all reconciliation checks passed | `dbt/`, `reconciliation/` |
| Airflow | both DAGs executed; 12 operational and 10 batch tasks succeeded | `airflow/` |
| Observability | seven Prometheus targets up; 33 dashboard expressions valid; one seven-span cross-Kafka trace captured | `prometheus/`, `grafana/`, `opentelemetry/` |
| Kubernetes/Helm/KEDA | local install, upgrade, rollback, pod recovery, and lag scaling executed | `kubernetes/`, `helm/`, `keda/` |
| Performance | 1K, 10K, three-tenant, and 20K backlog profiles recorded | `performance/` |

`runtime_manifest.json` is the machine-readable index and status boundary.
