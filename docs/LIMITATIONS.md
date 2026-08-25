# Current Boundaries

This document separates implemented behavior from deployment and verification
work that remains outside the local project scope.

## Deployment

- Docker Compose and a local `kind` cluster are the verified runtime targets.
- The Helm chart has been linted, rendered, and exercised locally.
- Terraform defines an AWS/EKS target and passes `fmt` and `validate`. It has
  not been applied, and the project has no AWS deployment.
- The Kubernetes verification reached 8/8 ready workloads and completed an
  authenticated ingestion-to-PostgreSQL round trip.
- KEDA 2.20.2 and its Kafka-lag `ScaledObject` were exercised locally. A
  bounded 20,000-event submission produced lag of 17,000; processing scaled
  from one replica to five, drained the lag, and returned to one replica.

## Authentication and Administrative Interfaces

- `AUTH_MODE=strict` is the code default.
- The checked-in Compose environment selects `AUTH_MODE=dev_compat` for local
  header-based examples. In this mode, callers choose their identity headers.
- The demo dashboard and ops console are local interfaces without
  authentication.
- The Compose demo dashboard uses `platform_tenant_scoped`, sets tenant
  context transaction-locally for protected-table reads, and is subject to
  PostgreSQL RLS. The Kubernetes and Helm packages do not deploy this service.
- The ops console has cross-tenant visibility through the
  `platform_admin_bypass` database role.
- The local HS256 issuer is a development path. OIDC/JWKS verification is
  implemented and tested against local Keycloak, but token lifecycle,
  revocation, and identity-provider operations are outside this repository.

## Data and Analytics

- Initialization data exists to make local services and reconciliation usable
  on a fresh database. Initialization terminology remains in schema and test
  documentation where it describes that mechanism.
- Reconciliation covers revenue, payment, and customer activity. It compares
  `tenant_metrics_daily` with independent calculations over
  `processed_orders`, `processed_payments`, and `processed_user_sessions`.
- `stream_window_metrics` has no reconciliation job.
- `product_performance` and `marketing_performance` have no dedicated stored-
  aggregate reconciliation check.
- Payment and customer-activity count tolerances are local defaults and have
  not been calibrated from operating traffic.

## Streaming

- Spark transformation logic is covered by 43 focused tests.
- The containerized streaming job launched all five queries in the bounded
  local run after its image, runtime identity, checkpoint permissions, and
  query-planning defects were corrected.
- Streaming metrics, lineage hooks, Kafka input, PostgreSQL sinks, watermark
  classifications, DLQ routing, and checkpoint restart were exercised locally.
- The asynchronous consumer and Spark streaming path write separate serving
  tables; `/metrics/revenue` reads the asynchronous path.

## Reliability

- Eight deterministic scenarios cover poison events, duplicates, late events,
  consumer lag, database and Redis outages, reconciliation mismatch, and
  consumer interruption.
- The poison-event scenario's direct Kafka publish step can be `not_run` when
  the local `kafka-python` installation is incompatible with Python 3.12. Its
  classification and DLQ-routing checks run independently.
- A local ingestion-pod termination was exercised; its Deployment replacement
  became Ready in three seconds. This is controller recovery, not production
  high-availability evidence.

## Performance

- Benchmark results are single-host local measurements and vary with host
  contention.
- The comparison gate uses a checked-in local ingestion baseline. It is kept
  out of CI because shared-runner performance is variable.
- k6 scripts are manual tools and are not part of the Makefile or CI workflow.
- A bounded three-tenant load and Kafka-backlog/KEDA drain were measured. No
  per-tenant fairness, multi-machine capacity, or production-throughput result
  is available.

## Observability

- Prometheus, Grafana provisioning, alert rules, and OpenTelemetry tracing are
  configured for local use.
- Grafana datasource and dashboard provisioning were checked through its API;
  Prometheus parsed all 33 expressions and 25 returned data in the bounded run.
- OpenTelemetry instrumentation and W3C Kafka propagation are implemented,
  tested, and verified with one local seven-span trace across ingestion and
  processing. The temporary backend is not part of default Compose, and
  PostgreSQL client spans remain uninstrumented.
- Alert thresholds and SLO targets are local defaults rather than operating
  commitments.

## Data Products and Lineage

- Consumer and ownership labels are domain-model metadata.
- Proxy metrics remain labeled as proxies or signals by contract validation.
- Code-reference validation applies to the nodes listed in
  `VERIFIABLE_NODE_SOURCES`; descriptive nodes receive structural validation.
- Runtime lineage events are emitted by backfill, reconciliation, and the Spark
  sink. dbt and data-quality runs do not emit lineage events.

## Backup and Recovery

- PostgreSQL backup tooling uses `pg_dump`/`pg_restore` and verifies per-table
  row counts in a scratch database.
- Verification does not compare row content.
- Backup execution is manual; there is no scheduled cadence, WAL archiving,
  point-in-time recovery, or cross-region recovery.
- Kafka, Redis, and MinIO do not have snapshot/restore tooling in this project.

## SDK and Developer Workflow

- The repository includes a Python producer SDK. It has no analytics-query
  client and no token minting or refresh support.
- Retries cover transport errors and 5xx responses. `429 Retry-After` handling
  is not implemented.
- The SDK is not published as a package and has no multi-language variants.
- `make ci-local` omits the CI service-image build; the equivalent Docker build
  command is documented in the workflow.
- The project uses Makefile and CI checks rather than a pre-commit framework.

## Incident Analysis

- The default incident-analysis provider is deterministic and offline.
- The external provider class is an extension point whose `analyze` method is
  not implemented.
- All analyses require human approval and have no command or infrastructure
  mutation path.

Capability-specific status and command records are indexed in
[../evidence/README.md](../evidence/README.md).
