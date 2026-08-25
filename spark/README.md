# Spark Processing Layer

Spark jobs cover the workloads that should not live inside request/consumer services:

- `streaming_enrichment.py` reads Kafka topics, adds processing metadata and event-domain partitions, and writes tenant/date partitioned parquet.
- `event_normalization_job.py` rebuilds normalized event files from Postgres raw events for replay or object-storage export.
- `batch_revenue_aggregates.py` recomputes daily revenue metrics from raw order events and writes a staging table for merge/reconciliation.
- `sessionization_job.py` builds user session summaries from activity events.
- `lakehouse_compaction.py` compacts bronze event parquet into deduplicated silver partitions in MinIO or S3.

The processing service maintains near-real-time hourly/daily aggregates for API freshness. Spark is used for backfills, reconciliation, sessionization, larger joins, and historical rebuilds.
