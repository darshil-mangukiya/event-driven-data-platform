# Scaling Strategy

## Local Tested Scale

The included load script is designed for realistic local testing, such as 1,000 to 50,000 events per run depending on machine resources.

## Production Scale Concept

For 10K to 1M events per minute, scale by:

- Increasing Kafka partitions per high-volume domain topic.
- Running multiple processing consumers in the same consumer group.
- Keeping event handlers idempotent through natural-key upserts.
- Moving historical rebuilds and heavy joins to Spark.
- Partitioning Postgres high-volume tables by time and clustering by tenant.
- Maintaining serving aggregates instead of querying raw events in APIs.
- Using Redis for hot metrics and dashboard refreshes.
- Adding backpressure, retry budgets, and DLQ replay tooling.
- Writing replayable bronze/silver parquet to S3 or MinIO while keeping PostgreSQL as the low-latency serving layer.

## Bottlenecks to Watch

- Kafka broker disk throughput and partition balance.
- Processing consumer lag and Postgres write contention.
- Tenant aggregate upsert hotspots.
- Analytics query latency for wide date ranges.
- Redis memory pressure from high-cardinality cache keys.
