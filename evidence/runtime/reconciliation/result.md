# Reconciliation Runtime Result

Status: **EXECUTED AND VERIFIED**.

Revenue, payment, and customer reconciliation ran across three dates: nine
checks passed with zero deltas. Kafka-to-dbt reconciliation then evaluated 14
rows across seven tenant/date pairs with zero missing or critical mismatches.
The first cross-tenant run correctly failed under the tenant-scoped role; it was
rerun through the designed local owner/admin path.

The broad seven-day data-quality evaluation passed seven checks, raised one
non-critical volume warning, and scored 92.5. The warning remains visible.
