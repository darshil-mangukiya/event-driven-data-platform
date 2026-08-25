# MinIO / S3-Style Lakehouse Path

Docker Compose includes MinIO so the platform can demonstrate an object-storage path alongside PostgreSQL serving tables.

Local endpoints:

- S3 API: http://localhost:9000
- MinIO console: http://localhost:9001
- Bucket: `data-platform`
- Access key: `platform`
- Secret key: `local-minio-secret-change-me`

Recommended layout:

```text
s3a://data-platform/
  raw/events/tenant_id=<tenant>/event_date=<date>/event_domain=<domain>/
  bronze/events/tenant_id=<tenant>/event_date=<date>/event_domain=<domain>/
  silver/orders/tenant_id=<tenant>/event_date=<date>/
  silver/payments/tenant_id=<tenant>/event_date=<date>/
  silver/user_sessions/tenant_id=<tenant>/event_date=<date>/
  gold/tenant_metrics/tenant_id=<tenant>/metric_date=<date>/
```

Zone intent:

- `raw`: original envelope archive for replay and diagnostic review.
- `bronze`: parsed event envelopes with partition metadata.
- `silver`: normalized domain facts.
- `gold`: aggregate outputs that can rebuild PostgreSQL serving tables.

Partitioning strategy:

- Partition by `tenant_id` and date first.
- Add `event_domain` for mixed event folders.
- Avoid extremely high-cardinality partitions such as `event_id`.

Retention strategy:

- Keep raw replay archives longer than API audit logs.
- Archive or compact older bronze/silver partitions before deletion.
- Keep gold outputs aligned with metric SLA and reconciliation needs.

Spark writes tenant/date/domain partitioned parquet and can compact small files with:

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  spark/jobs/lakehouse_compaction.py
```

PostgreSQL remains the serving layer for APIs. MinIO represents the replayable lakehouse storage layer for backfills, larger analytical jobs, and long-term retention.

This project does not implement Delta Lake, Iceberg, or Hudi table formats. Those are practical future production upgrades when the platform needs ACID table metadata, schema evolution, deletes, compaction services, and catalog integration at larger scale.
