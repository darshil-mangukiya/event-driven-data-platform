# Kafka Event Design

Topics are split by event domain instead of using one giant bus. That keeps consumer contracts small, lets the platform tune retention and partition counts per workload, and prevents high-volume user activity from starving lower-volume system events.

Partitioning uses `tenant_id` plus the strongest available business key (`order_id`, `payment_id`, `user_id`, or `product_id`). This preserves per-tenant ordering for the same business entity while still distributing large tenants across partitions.

Retry behavior:

- Validation failures are rejected by the ingestion service before Kafka.
- Transient processing failures should be republished to `platform.events.retry` with attempt metadata.
- Poison messages go to `platform.events.dlq` with the original event and failure stage.
- DLQ records are retained longer for replay, audit, and root-cause analysis.

Local replication factor is `1` for developer laptops. Production should use replication factor `3`, rack-aware brokers, topic ACLs, and consumer lag alerts.

