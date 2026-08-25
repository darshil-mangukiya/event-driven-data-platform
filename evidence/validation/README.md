# Generated Validation Artifacts

This directory contains generated summaries and curated runtime records. See
[the verification index](../README.md) for current capability status.

Generated summary files:

Generated files:

- `capability_matrix.json`
- `service_health_summary.json`
- `sample_api_responses.json`
- `benchmark_summary.json`
- `data_quality_summary.json`
- `reconciliation_summary.json`
- `docker_services.json`
- `test_result_summary.json`

| Capability | Evidence | Tests |
| --- | --- | --- |
| Microservices | `services/ingestion-service`<br>`services/processing-service`<br>`services/analytics-service`<br>`services/metadata-service`<br>`services/ops-console` | `tests/test_api_contracts.py` |
| Event contracts and compatibility | `contracts/registry.json`<br>`contracts/schemas/v1`<br>`contracts/schemas/v2`<br>`scripts/check_contract_compatibility.py` | `tests/test_event_contracts.py`<br>`tests/test_reliability_governance_tooling.py` |
| Replay safety | `docs/idempotency-replay-safety.md`<br>`services/processing-service/app/worker.py`<br>`services/processing-service/app/repository.py` | `tests/integration/test_event_processor_flow.py` |
| Outbox/inbox operations | `sql/outbox`<br>`scripts/outbox_dispatch_plan.py`<br>`docs/outbox-inbox-pattern.md` | `tests/test_platform_hardening_tooling.py` |
| Governance and privacy | `governance/pii_classification.json`<br>`sql/privacy/tenant_erasure_plan.sql`<br>`docs/privacy-governance.md` | `tests/test_platform_hardening_tooling.py` |
| Release readiness | `scripts/platform_preflight.py`<br>`docs/release-readiness.md` | `tests/test_platform_packaging.py` |
| Tenant onboarding and platform CLI | `platform_cli`<br>`docs/tenant-onboarding.md`<br>`examples/internal_consumers/team_api_consumers.py` | `tests/test_platform_cli_and_tenant_onboarding.py` |
| Traceability and checkpointing | `database/migrations/versions/0005_traceability_watermarks_audit.py`<br>`database/init/001_schema.sql` | `tests/test_platform_cli_and_tenant_onboarding.py`<br>`tests/test_event_contracts.py` |
