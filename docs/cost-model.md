# Cost Model

This is a planning model, not live cloud pricing. Actual spend depends on region, retention, reserved capacity, data transfer, and workload shape.

## Cost Drivers

| Driver | Why it matters | Control strategy |
| --- | --- | --- |
| Kafka throughput and retention | Broker count, partition count, replication, and retention dominate event backbone cost. | Keep domain topics compact, set retention by replay need, and move long-term history to object storage. |
| Postgres write volume | Serving tables receive event-derived writes and query traffic from APIs. | Pre-aggregate hot metrics, index tenant/time paths, archive raw history, and use read replicas for BI bursts. |
| Spark job runtime | Batch backfills and sessionization cost by executor size and elapsed time. | Run scheduled jobs in bounded windows, use partition pruning, and separate routine jobs from exceptional rebuilds. |
| Redis cache size | Hot tenant dashboards and metric responses use memory. | Cache compact API payloads, use short TTLs, and avoid caching high-cardinality ad hoc queries. |
| Observability volume | Logs, metrics, and traces grow with service count and event throughput. | Sample verbose traces, retain high-value metrics, and keep structured logs concise. |
| NAT and cross-AZ traffic | Data movement can become a quiet bill driver. | Co-locate services by region/AZ where possible and avoid unnecessary cross-zone chatter. |

## Local Development

| Component | Local shape | Cost |
| --- | --- | ---: |
| FastAPI services | Docker Compose containers | $0 cloud spend |
| Kafka | Single local broker | $0 cloud spend |
| Postgres | Single local container | $0 cloud spend |
| Redis | Single local container | $0 cloud spend |
| Spark | Local master/worker containers | $0 cloud spend |
| Prometheus + Grafana dashboard JSON | Local observability assets | $0 cloud spend |

Local development cost is machine time and disk usage. This mode is enough to demonstrate architecture, contracts, local benchmarks, and API behavior.

## Cloud MVP Estimate

Assumptions: one production-like environment, moderate traffic, short Kafka retention, one Postgres primary, small Redis cache, containerized services, batch Spark scheduled only when needed.

| Layer | Typical managed-service choice | Monthly planning range |
| --- | --- | ---: |
| Compute for services | ECS/Fargate or EKS nodes | $150-$700 |
| Kafka | MSK or Confluent basic cluster | $300-$1,500 |
| Postgres | RDS PostgreSQL, single primary | $200-$900 |
| Redis | ElastiCache small replication group | $80-$400 |
| Object storage | S3/MinIO-compatible history | $20-$200 |
| Spark batch | EMR/Glue scheduled jobs | $50-$800 |
| Observability | CloudWatch/managed Prometheus/Grafana | $100-$700 |
| Network/data transfer | NAT, cross-AZ, egress | $50-$600 |

Planning range: roughly `$950-$5,800/month` before enterprise support, reserved-instance discounts, or high availability expansion.

## Growth Environment Estimate

Assumptions: higher event volume, multi-AZ Kafka, larger Postgres, read replica, bigger Redis cache, more frequent Spark jobs, longer observability retention.

| Layer | Scaling change | Monthly planning range |
| --- | --- | ---: |
| Compute | More service replicas and autoscaling headroom | $600-$3,000 |
| Kafka | More brokers, partitions, and retention | $1,500-$8,000 |
| Postgres | Larger primary plus read replica | $1,000-$6,000 |
| Redis | Multi-node cache or larger memory tier | $400-$2,000 |
| Object storage | Larger raw/normalized history | $100-$1,000 |
| Spark batch | Daily/hourly jobs and backfill headroom | $800-$8,000 |
| Observability | Higher-cardinality metrics and logs | $500-$4,000 |
| Network/data transfer | NAT, private links, cross-AZ replication | $500-$5,000 |

Planning range: roughly `$5,400-$37,000/month`, mostly driven by Kafka, database sizing, Spark runtime, and observability volume.

## Cost-Aware Architecture Choices

- Keep Postgres as a serving layer, not a raw-event lake.
- Land long-retention raw and normalized events in object storage.
- Use tenant/date partitioning and serving aggregates before adding broad indexes.
- Cache only predictable dashboard/API responses with bounded cardinality.
- Treat backfills as scheduled work with explicit windows, not always-on compute.
- Store high-cardinality debugging fields in logs/traces only when they are useful for operations.

## FinOps Metrics To Add In Production

| Metric | Owner | Decision enabled |
| --- | --- | --- |
| Cost per million events ingested | Platform | Kafka and ingestion efficiency. |
| Cost per tenant dashboard load | Analytics platform | Cache and serving-query effectiveness. |
| Spark cost per backfilled day | Data engineering | Batch job tuning and partition strategy. |
| Postgres write amplification | Platform/database | Aggregate design and index tradeoffs. |
| Observability GB per service | SRE/platform | Log sampling and retention policy. |
