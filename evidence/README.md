# Verification Index

This directory contains curated command records for implemented capabilities.
The current bounded execution index is the
[local runtime manifest](runtime/runtime_manifest.json). Records under
`evidence/validation/` include earlier runs and remain useful for history; when
a status differs, the runtime manifest and its explicit limitations are the
current public claim boundary.

Statuses describe the scope captured in each record:

- **VERIFIED** — executed against the stated local dependency or runtime;
- **TEST VERIFIED** — exercised through automated tests;
- **CONFIGURATION VERIFIED** — parsed, linted, rendered, or accepted without a
  complete runtime behavior cycle;
- **ENVIRONMENT LIMITED** — implementation checks pass, while a runtime
  dependency blocks the recorded live path;
- **NOT EXECUTED** — no execution record exists for the stated behavior.

## Platform and Data Flow

| Capability | Status | Record |
| --- | --- | --- |
| At-least-once idempotency, 100 offsets to one row | VERIFIED | [Idempotency](validation/idempotency-verification.md) |
| dbt build | VERIFIED | [dbt live verification](validation/dbt-live-verification.md) |
| Kafka/dbt metric comparison | VERIFIED | [Kafka/dbt reconciliation](validation/kafka-dbt-metric-reconciliation.md) |
| Revenue, payment, and customer-activity reconciliation | VERIFIED | [Current reconciliation](runtime/reconciliation/result.md) |
| Backup and restore tooling | TEST VERIFIED | [Recovery runbook](../docs/disaster-recovery-runbook.md) |

## Security

| Capability | Status | Record |
| --- | --- | --- |
| Strict authentication default | TEST VERIFIED | [Security model](../docs/security.md) |
| OIDC/JWKS verification | VERIFIED | [OIDC](validation/oidc-verification.md) |
| RLS policy and role matrix | VERIFIED | [RLS runtime](validation/rls-runtime-verification.md) |
| Application runtime under scoped roles | VERIFIED | [Application RLS](validation/application-rls-runtime-verification.md) |
| RLS on fresh initialization | VERIFIED | [Fresh initialization](validation/rls-fresh-init-verification.md) |

## Contracts and Governance

| Capability | Status | Record |
| --- | --- | --- |
| Schema registry behavior | VERIFIED | [Schema Registry](validation/schema-registry-verification.md) |
| AsyncAPI generation and references | VERIFIED | [AsyncAPI](validation/asyncapi-verification.md) |
| Data-product contracts | TEST VERIFIED | [Data products](../docs/data-products.md) |
| Catalog and lineage graph | TEST VERIFIED | [Lineage](../docs/lineage.md) |
| Producer SDK authentication and retry behavior | TEST VERIFIED | [SDK](../sdk/python/README.md) |

## Streaming, Reliability, and Observability

| Capability | Status | Record |
| --- | --- | --- |
| Spark transformation logic | VERIFIED | [Current streaming runtime](runtime/streaming/RESULT.md) |
| Spark container launch and checkpoint restart | VERIFIED | [Current streaming runtime](runtime/streaming/RESULT.md) |
| Reliability scenarios | TEST VERIFIED | [Reliability](../docs/reliability.md) |
| Redis outage fallback | VERIFIED | [Redis degradation](validation/redis-degradation-performance.md) |
| Redis cache behavior | TEST VERIFIED | [Caching strategy](../docs/caching-strategy.md) |
| Prometheus and Grafana local runtime (bounded data coverage) | VERIFIED | [Prometheus](runtime/prometheus/PROMETHEUS_VALIDATION.md), [Grafana](runtime/grafana/result.md) |
| SLO and backpressure configuration | CONFIGURATION VERIFIED | [SLO catalog](../docs/slo-catalog.md) |
| OpenTelemetry instrumentation and Kafka propagation | VERIFIED LOCALLY | [Current trace](runtime/opentelemetry/TRACE_VALIDATION.md) |
| Incident analysis | TEST VERIFIED | [Control boundaries](../ai_incident_copilot/AI_CONTROL_BOUNDARIES.md) |

## Platform Packaging

| Capability | Status | Record |
| --- | --- | --- |
| Raw Kubernetes manifests | VERIFIED | [Kubernetes](validation/kubernetes-verification.md) |
| In-cluster event round trip | VERIFIED | [Kubernetes E2E](validation/kubernetes-e2e-verification.md) |
| Helm chart lifecycle | VERIFIED | [Current Helm lifecycle](runtime/helm/result.md) |
| KEDA operator and `ScaledObject` | VERIFIED | [Current KEDA result](runtime/keda/result.md) |
| KEDA scaling cycle | VERIFIED | [Autoscaling timeline](runtime/keda/autoscaling_timeline.csv) |
| Airflow operational and batch DAGs | VERIFIED | [Current Airflow result](runtime/airflow/result.md) |
| Terraform AWS/EKS target | CONFIGURATION VERIFIED | [Terraform](validation/terraform-verification.md) |

## Developer Workflow

| Capability | Status | Record |
| --- | --- | --- |
| Unit, contract, integration, and Spark tests | TEST VERIFIED: 466 passed, 9 skipped | [Testing strategy](../docs/testing-strategy.md) |
| Coverage report | VERIFIED | [Coverage](validation/test-coverage-report.md) |
| Platform validator | VERIFIED | [Platform validation](validation/platform-validation.md) |
| Release checks | VERIFIED | [Release readiness](validation/release-readiness.md) |
| Ops console | TEST VERIFIED | [Ops console](../docs/ops-console.md) |
| Local performance gate | TEST VERIFIED | [Benchmark report](../docs/BENCHMARK_REPORT.md) |

The JSON and Markdown summaries under `evidence/validation/` are checked by
`scripts/validate_evidence_consistency.py`. Runtime records may describe older
local environments; current repository validation should be taken from a fresh
test and validator run.
