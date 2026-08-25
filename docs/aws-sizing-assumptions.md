# AWS Sizing Assumptions

Initial MVP:

- ECS/Fargate for FastAPI services
- MSK or Confluent Cloud for Kafka
- RDS PostgreSQL for serving and operations tables
- ElastiCache Redis for API caching
- S3 for raw/normalized history
- Glue/EMR or containerized Spark for scheduled batch jobs

Growth assumptions:

| Layer | Starting point | Growth trigger |
| --- | --- | --- |
| API services | 2 tasks per service | p95 latency or CPU saturation |
| Kafka | 3 broker multi-AZ | sustained partition lag or broker disk pressure |
| Postgres | provisioned RDS primary | write latency, read QPS, or storage growth |
| Redis | small replication group | hot dashboard QPS or cache memory pressure |
| Spark | scheduled job cluster | backfill windows exceeding SLA |

Sizing should be revisited after measured event volume, tenant count, and dashboard concurrency are known.
