# Governance and Data Management

## Data Classification

| Class | Examples | Handling |
| --- | --- | --- |
| Public metadata | Product categories, service names | Safe in dashboards and docs. |
| Internal business data | Revenue, orders, campaign performance | Tenant-scoped access and audit logging. |
| Sensitive customer data | Emails, customer IDs, payment risk signals | Minimize logging, mask exports, restrict replay access. |
| Operational secrets | JWT secret, DB password, access keys | Environment or secret manager only; never commit real values. |

## Metric Ownership

- Product Analytics owns activation, retention, and product performance definitions.
- Finance owns revenue and payment success definitions.
- Marketing owns campaign attribution and ROI assumptions.
- Platform owns event contracts, data quality checks, and serving SLOs.

## Quality Gates

- Required event envelope fields must be present.
- Metrics cannot be negative where business logic forbids it.
- Freshness checks should alert when raw events stop arriving.
- Volume anomalies are warnings unless confirmed as incidents.
- dbt-style tests validate mart-level assumptions.

## Audit Requirements

- API usage is recorded in `api_usage_log`.
- Replay operations are recorded in `dlq_replay_audit`.
- Pipeline runs are recorded in `pipeline_run_log`.
- Data quality outcomes are recorded in `data_quality_check_results` and `data_quality_score_daily`.
- Privacy subject requests are recorded in `privacy_erasure_requests`.
- Lineage events are recorded in `lineage_events`.

## Retention Policy

- Hot metrics: retained in PostgreSQL for fast serving.
- Raw events: retained in PostgreSQL for short operational windows and archived to lakehouse storage.
- DLQ records: retained long enough for replay and incident review.
- API usage logs: retained for audit, usage analytics, and chargeback.

See `docs/privacy-governance.md` for PII classification, masking, retention, and erasure workflow details.
