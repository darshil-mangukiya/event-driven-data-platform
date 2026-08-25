# Authoritative Platform Validation

Overall status: **VERIFIED**

| Check | Status | Detail |
| --- | --- | --- |
| ruff_lint | VERIFIED | ruff check --no-cache . |
| event_contracts | VERIFIED | .venv/bin/python scripts/validate_event_contracts.py |
| contract_compatibility | VERIFIED | .venv/bin/python scripts/check_contract_compatibility.py |
| catalog | VERIFIED | .venv/bin/python scripts/validate_catalog.py |
| metric_contracts | VERIFIED | .venv/bin/python scripts/validate_metric_contracts.py |
| privacy_catalog | VERIFIED | .venv/bin/python scripts/validate_privacy_catalog.py |
| schema_drift | VERIFIED | .venv/bin/python scripts/schema_drift_report.py |
| rls_static | VERIFIED | .venv/bin/python scripts/validate_tenant_rls.py |
| lineage | VERIFIED | .venv/bin/python scripts/validate_lineage.py |
| data_products | VERIFIED | .venv/bin/python scripts/validate_data_products.py |
| asyncapi | VERIFIED | .venv/bin/python scripts/validate_asyncapi.py |
| evidence_consistency | VERIFIED | .venv/bin/python scripts/validate_evidence_consistency.py --skip-test-count |
| auth_posture | VERIFIED | .venv/bin/python scripts/validate_auth_posture.py |
| compose_config | VERIFIED | docker compose -f docker-compose.yml config --quiet |
| compose_streaming_config | VERIFIED | docker compose -f docker-compose.yml --profile streaming config --quiet |
| terraform_fmt | VERIFIED | terraform -chdir=infra/aws/terraform fmt -check |
| helm_lint | VERIFIED | helm lint deploy/helm/cloudscale |
| ai_incident_copilot_importable | VERIFIED | .venv/bin/python -c import sys; sys.path.insert(0,'.'); sys.path.insert(0,'services/shared'); import ai_incident_copilot.copilot |
| kafka_schema_registry_runtime | VERIFIED | Registry compatibility fixtures were exercised against a running local service. |
| kubernetes_execution | VERIFIED | All 8 packaged workloads reached Ready in a local kind cluster. |
| helm_packaging | VERIFIED | Deployed from the packaged chart, matched the raw-manifest result. |
| keda_autoscaling | CONFIGURATION_ONLY | Operator and ScaledObject accepted and lag readable; scale-up and scale-down were not observed. |
| terraform_aws_target | VERIFIED | fmt/validate clean; no apply, no AWS resources provisioned. |
| oidc_jwks | VERIFIED | Verified against local Keycloak, including rejection cases. |
| rls_runtime_enforcement | VERIFIED | Full live test matrix passed after two runtime defects were identified and corrected. |
| opentelemetry_tracing | VERIFIED | A continuous trace crossed the Kafka boundary and was observed in local Jaeger. |
| ai_incident_copilot_controls | VERIFIED | Offline provider only; schema validation and human-approval controls are covered by tests. |
| postgres_performance_audit | VERIFIED | Existing indexing confirmed used (Index Scan, not Seq Scan) via live EXPLAIN ANALYZE. |
| redis_degradation | VERIFIED | Cache fallback behavior and degradation measurements are recorded. |
| kafka_dependency_metrics | VERIFIED | Consumer-lag instrumentation and alert configuration are covered by tests. |

Counts by status: `{"CONFIGURATION_ONLY": 1, "VERIFIED": 29}`

Fast checks run in the current invocation. Live-infrastructure capabilities
are summarized from checked-in `evidence/validation/*.md` records and are
not re-executed by this aggregate validator.
