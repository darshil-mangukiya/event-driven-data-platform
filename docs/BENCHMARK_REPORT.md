# Benchmark Report

## Scope

This report documents local benchmark evidence and production scaling assumptions separately. The local project is designed to validate architecture, contracts, tenant isolation, and operational workflows. It does not claim production event volume from a laptop-sized Docker Compose environment.

## Local Machine Assumptions

Local validation assumes:

- Docker Compose stack running on a developer machine.
- Single Kafka broker.
- Single PostgreSQL instance.
- Single Redis instance.
- One instance of each FastAPI service.
- Local workload events created by project scripts.
- No distributed load generator.

## Tested Local Workloads

| Workload | Tool | Purpose |
| --- | --- | --- |
| Ingestion batch load | `scripts/load_test_events.py` | Measures API ingestion latency and accepted event count for local batches. |
| Analytics API read load | `benchmarks/k6/analytics_read_load.js` | Exercises tenant-scoped metric endpoints under repeated reads. |
| Ingestion API burst | `benchmarks/k6/ingestion_batch_load.js` | Exercises `/events/batch` with tenant-scoped development payloads. |
| Benchmark comparison | `scripts/compare_benchmarks.py` | Compares local result JSON files against sample baselines. |

## Sample Evidence

The repository includes `samples/benchmarks/local_ingestion_sample.json` as example evidence format. It is intentionally labeled as sample local evidence.

| Metric | Example Value |
| --- | ---: |
| Total events | 5,000 |
| Failure count | 0 |
| Events per second | 594.0244 |
| p95 latency | 134.88 ms |

These values mirror `samples/benchmarks/local_ingestion_sample.json` and
describe the included local sample only.

## Bottlenecks to Watch

- Kafka producer acknowledgement latency during larger batches.
- PostgreSQL aggregate upserts under high tenant cardinality.
- Redis cache hit rate on dashboard endpoints.
- Python worker concurrency for CPU-heavy payload validation.
- Spark job startup overhead for small local backfills.

## Query Optimization Notes

Important local indexes:

- `raw_events (tenant_id, event_timestamp desc)`
- `raw_events (tenant_id, correlation_id, event_timestamp desc)`
- `raw_events (tenant_id, idempotency_key)`
- `processed_orders (tenant_id, event_timestamp desc)`
- `processed_orders (tenant_id, product_id, event_timestamp desc)`
- `processed_payments (tenant_id, status, event_timestamp desc)`
- `tenant_metrics_daily primary key (tenant_id, metric_date)`
- `api_usage_log (tenant_id, requested_at desc)`
- `pipeline_watermarks (status, updated_at desc)`

## Production Scaling Path

Production validation would require:

- Managed Kafka or a multi-broker Kafka cluster.
- Partition count sized by tenant and domain throughput.
- Horizontally scaled ingestion and processing services.
- Managed PostgreSQL with read replicas and partitioning.
- Distributed k6 or Locust workers.
- Cloud object storage for replay/backfill archive.
- Dedicated observability backend for traces, metrics, logs, and SLO alerts.

## Measurement boundary

The benchmark scripts and included output cover local workloads. Production
volume and distributed-load measurements have not been executed.
