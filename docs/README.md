# Documentation

Engineering documentation for the local event-driven data platform.

| Document | Purpose |
| --- | --- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | System architecture, local components, and implemented-vs-production-dependent capabilities. |
| [SERVICE_CATALOG.md](SERVICE_CATALOG.md) | Service ownership, endpoints, dependencies, tables, topics, and failure modes. |
| [EVENT_CONTRACTS.md](EVENT_CONTRACTS.md) | Kafka topics, event envelope design, schema rules, compatibility checks, and DLQ behavior. |
| [SECURITY_AND_TENANCY.md](SECURITY_AND_TENANCY.md) | Auth flow, RBAC roles, tenant isolation, audit logging, and RLS path. |
| [DATA_QUALITY.md](DATA_QUALITY.md) | Data quality checks, result tables, commands, and reconciliation links. |
| [OBSERVABILITY.md](OBSERVABILITY.md) | Health checks, metrics, alerts, evidence tables, and observability boundaries. |
| [BENCHMARK_REPORT.md](BENCHMARK_REPORT.md) | Local benchmark scope, sample results, bottlenecks, and production scaling notes. |
| [RUNBOOK.md](RUNBOOK.md) | Local triage, DLQ replay, backfill, recovery assumptions, and operational commands. |
| [LOCAL_VERIFICATION.md](LOCAL_VERIFICATION.md) | Local smoke checks, Docker verification commands, service pages, and runtime evidence checks. |
| [DESIGN_DECISIONS.md](DESIGN_DECISIONS.md) | Engineering decisions behind the local architecture. |
| [LIMITATIONS.md](LIMITATIONS.md) | Local runtime, data, security, observability, and load-testing limitations. |
| [../airflow/README.md](../airflow/README.md) | Optional local Airflow orchestration for operational checks and batch workflows. |

Additional reference docs cover ADRs, API notes, caching, query optimization, governance, privacy, lineage, release readiness, deployment hardening, and operational workflows.
