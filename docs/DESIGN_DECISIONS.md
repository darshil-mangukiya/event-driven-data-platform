# Design Decisions

This document records engineering decisions for the local data-platform implementation.

## Domain-Based Kafka Topics

Kafka topics are split by event domain: orders, payments, users, products, system, retry, and DLQ. Domain topics keep schemas and ownership smaller than a single shared event bus, and they allow retention, partitioning, and consumer behavior to be tuned per workload.

## PostgreSQL As The Local Serving Layer

PostgreSQL is used for local analytics serving because it provides predictable SQL behavior, straightforward indexing, and a simple local runtime. Raw events, processed facts, serving aggregates, metadata, audit logs, and operational tables can be inspected with standard SQL.

## Redis For Metric Caching

Redis is used for repeated tenant-scoped metric responses and lightweight rate-limit state. It is not a source of truth. PostgreSQL remains the local serving source, and cache invalidation is TTL-first with explicit clearing after urgent backfills.

## Shared-Schema Multi-Tenancy

The local implementation uses shared tables with `tenant_id` on core entities. This keeps the stack small and easy to run locally while still exercising tenant-aware APIs, SQL filters, cache keys, audit logging, and validation tests.

## Tenant Isolation

Tenant isolation combines tenant claims, tenant-aware route checks,
tenant-scoped SQL filters, tenant-specific Redis keys, API audit logs, and
PostgreSQL RLS. Tenant-facing Compose services use the non-superuser,
non-bypass role and set tenant context transaction-locally; cross-tenant tools
use a separate non-superuser bypass role.

## Event Contract Versioning

Event envelopes and domain payloads are represented with JSON Schema contracts, fixtures, and compatibility scripts. The default compatibility rule is backward compatibility: optional fields may be added, but removing required fields or changing field types requires an explicit migration.

## Replay And Backfill

DLQ replay and metric backfill workflows are represented through dry-run-capable CLI and scripts. The local implementation records audit information and emphasizes idempotency review before replaying or rebuilding metrics.

## Spark Job Boundary

Low-latency event processing stays in the processing service. PySpark jobs handle batch-oriented workloads such as normalization, sessionization, aggregate rebuilds, streaming-style enrichment, and lakehouse compaction.

## Airflow For Finite Operational Workflows

Airflow is added as an optional local orchestration layer because validation, reconciliation, metric backfill dry-runs, evidence generation, and Spark batch jobs are bounded workflows that benefit from scheduling and dependency tracking.

Airflow is not used for Kafka streaming consumers. Kafka ingestion and processing remain service-driven because those processes are long-running runtime services with their own health checks, consumer groups, retry paths, and DLQ behavior.

Workflows that belong in Airflow:

- contract and governance checks
- sample artifact validation
- schema drift reports
- reconciliation dry-runs
- metric backfill dry-runs
- finite PySpark batch jobs
- validation evidence refreshes

Workflows that stay in services or CLI:

- Kafka producers and consumers
- FastAPI service runtime
- Redis cache behavior
- ad hoc tenant onboarding and operator commands

The local Airflow setup uses a Compose overlay and local metadata database. It is useful for development and workflow validation, but production scheduling would need hardened executor configuration, remote logs, secrets, alerting, and deployment automation.

## Local Vs Production-Dependent Capabilities

Implemented locally:

- FastAPI services
- Kafka topic definitions and producer/consumer code
- PostgreSQL schema, seed data, and migrations
- Redis-backed cache usage
- Event and metric contract checks
- Tenant-aware APIs and tests
- Data quality, reconciliation, replay, and backfill tooling
- Optional Airflow DAGs for finite operational and batch workflows
- Prometheus configuration and Grafana dashboard JSON
- Docker Compose local stack

Production-dependent:

- Multi-broker or managed Kafka
- Managed PostgreSQL and Redis
- Centralized secrets management
- OIDC/JWKS identity integration
- OpenTelemetry tracing
- Distributed load testing
- Production Airflow executor, remote logs, alert routing, and secrets
- Production deployment and rollback automation
