# PostgreSQL Storage Layer

The serving database balances normalized event tables with API-ready aggregate tables:

- `raw_events` stores the original event envelope for audit, replay, and schema drift analysis.
- `processed_orders`, `processed_payments`, `processed_user_sessions`, and `tenant_products` hold validated domain records.
- `tenant_metrics_hourly` is updated by the Kafka processing service for near-real-time metrics.
- `tenant_metrics_daily` is the BI/API serving table and can also be rebuilt from hourly/raw data by Spark batch jobs.
- `event_outbox` and `event_inbox` support transactional publish and replay-safe consumption patterns.
- `alerts`, `service_health_metrics`, `pipeline_run_log`, and `api_usage_log` support platform observability.
- `dlq_replay_audit`, `data_quality_check_results`, `data_quality_score_daily`, `benchmark_run_results`, and `reconciliation_audit` support operations evidence.
- `lineage_events`, `privacy_erasure_requests`, and `data_retention_policies` support governance and audit workflows.

Local Docker uses ordinary tables for portability. In production, `raw_events` and high-volume processed tables should be range-partitioned by event date and optionally subpartitioned or clustered by `tenant_id`. The key query pattern is always `(tenant_id, time)`, so indexes follow that shape.
