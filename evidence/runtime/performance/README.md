# Performance Profiles

All profiles used the actual ingestion batch endpoint and deterministic seeds
and business payloads. The finalized generator also emits seeded event IDs.
Latencies below are whole-batch request completion latency, not
per-event end-to-end processing latency. Consumer lag was observed to drain to
zero. No baseline-versus-tuned claim is made because no equivalent controlled
tuning trial was performed.

| Profile | Events | Accepted events/s | p50 ms | p95 ms | p99 ms | Failed batches |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Smoke | 1,000 | 670.1300 | 1417.2949 | 1441.5733 | 1442.3259 | 0 |
| Small | 10,000 | 1748.1545 | 5493.6405 | 5607.1563 | 5674.3815 | 0 |
| Three-tenant noisy neighbor | 1,500 | 1783.5643 | 714.6334 | 787.8718 | 813.6025 | 0 |
| KEDA backlog | 20,000 | 1125.2795 | 17375.6956 | 17592.2632 | 17617.9016 | 0 |

The noisy-neighbor distribution was 80/10/10 across three synthetic tenants.
All batches succeeded, but the aggregate runner did not collect per-tenant
latency, so fairness is not proven. Medium/large profiles, a controlled Kafka
partition comparison, PostgreSQL EXPLAIN ANALYZE before/after evidence, and a
cache latency benchmark remain unexecuted rather than inferred.
