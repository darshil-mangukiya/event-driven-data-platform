# Architecture Visuals

## System Architecture

```mermaid
flowchart LR
    Teams["Internal teams"] --> Ingest["Ingestion Service"]
    Ingest --> Kafka["Kafka domain topics"]
    Kafka --> Processor["Processing Service"]
    Kafka --> SparkStream["Spark streaming enrichment"]
    Processor --> Postgres["PostgreSQL serving layer"]
    SparkBatch["Spark batch jobs"] --> Postgres
    SparkBatch --> S3["MinIO / S3 lakehouse"]
    Analytics["Analytics Service"] --> Postgres
    Analytics --> Redis["Redis cache"]
    Metadata["Metadata Service"] --> Postgres
    Dashboard["Demo Dashboard"] --> Postgres
    Prometheus["Prometheus"] --> Ingest
    Prometheus --> Processor
    Prometheus --> Analytics
    Prometheus --> Metadata
    Prometheus --> Dashboard
```

## Event Flow

```mermaid
sequenceDiagram
    participant Client
    participant Ingestion
    participant Kafka
    participant Processing
    participant Postgres
    participant Analytics
    Client->>Ingestion: POST /events
    Ingestion->>Ingestion: Validate envelope and payload
    Ingestion->>Kafka: Publish to domain topic
    Kafka->>Processing: Consume event
    Processing->>Postgres: raw_events
    Processing->>Postgres: processed domain table
    Processing->>Postgres: tenant_metrics_hourly/daily
    Client->>Analytics: GET /metrics/revenue
    Analytics->>Postgres: Tenant-scoped serving query
    Analytics-->>Client: Cached metric payload
```

## Tenant Isolation

```mermaid
flowchart TB
    Token["JWT or tenant headers"] --> Guard["Tenant access check"]
    Guard --> Query["tenant_id predicate in SQL"]
    Query --> Tables["Shared PostgreSQL tables"]
    Tables --> Views["Tenant-isolated serving responses"]
    Guard --> CacheKey["Redis key includes tenant_id"]
    CacheKey --> Cache["Tenant-scoped cache payloads"]
```

## Deployment Shape

```mermaid
flowchart TB
    subgraph Runtime["ECS/EKS or Docker Compose"]
      Ingestion["ingestion-service"]
      Processing["processing-service"]
      Analytics["analytics-service"]
      Metadata["metadata-service"]
      Dashboard["demo-dashboard"]
    end
    subgraph Managed["Managed/stateful services"]
      Kafka["MSK/Kafka"]
      Postgres["RDS/PostgreSQL"]
      Redis["ElastiCache/Redis"]
      Lake["S3/MinIO"]
    end
    Ingestion --> Kafka
    Kafka --> Processing
    Processing --> Postgres
    Analytics --> Postgres
    Analytics --> Redis
    Metadata --> Postgres
    Dashboard --> Postgres
    Processing --> Lake
```

## Data Lineage

See [Lineage and Data Catalog](lineage.md) for the table-level graph and ownership map.

