# Executed Reliability Summary

Status: **EXECUTED AND VERIFIED** for the scenarios listed.

| Scenario | Symptom | Recovery evidence | Boundary |
| --- | --- | --- | --- |
| Consumer redelivery | reset offset redelivered event | inbox skipped duplicate; duplicate DB effects 0 | one controlled event |
| Spark startup/restart | job failed on dependency/config/optimizer defects | fixes applied; five queries resumed checkpoint | bounded local job |
| Redis outage | cache unavailable | analytics stayed HTTP 200 via PostgreSQL, cache recovered | no latency SLO claim |
| Airflow command failure | lineage import failed | shared environment fixed; full operational DAG passed | bounded local executor |
| Airflow batch portability | compaction defaulted to unavailable S3A; empty seven-day window | local paths supplied; normalized data dependency enforced; all 10 tasks passed | retained synthetic data; dry-run maintenance tasks |
| Kubernetes pod termination | ingestion pod deleted | replacement Ready in 3 seconds | single-node local cluster |
| Kafka backlog | lag reached 17,000 | KEDA scaled 1 -> 5; lag reached 0; scaled to 1 | local synthetic load |
| Helm packaging defect | RLS/topic topology incomplete | chart fixed; install/upgrade/rollback and hook passed | not production rollout |

PostgreSQL was exercised through fail-closed permissions and Spark sink
availability, but a timed full database outage/recovery was not separately run.
