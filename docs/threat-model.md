# Threat Model

## Scope

This threat model covers tenant-scoped analytics APIs, event ingestion, Kafka processing, PostgreSQL storage, Redis caching, DLQ replay, and local JWT auth.

## Key Assets

- Tenant event payloads.
- Tenant metrics and business KPIs.
- JWT secrets and access tokens.
- Raw events and replayable lakehouse data.
- DLQ records containing original event payloads.
- API usage logs and audit data.

## Threats and Mitigations

| Threat | Risk | Mitigation |
| --- | --- | --- |
| Cross-tenant data access | A user queries another tenant's metrics. | API tenant checks, tenant-scoped SQL, optional RLS policy scripts, tests for access logic. |
| Token forgery | A caller creates a forged tenant token. | Signed JWTs, issuer/audience checks, secret management through environment or Kubernetes secrets. |
| DLQ replay abuse | A bad event is replayed repeatedly or to the wrong topic. | Replay audit table, dry-run mode, event-id targeting, operator identity, target topic logging. |
| PII leakage in logs | Sensitive payload fields appear in logs or DLQ errors. | Structured logs avoid full payload dumps; governance docs define masking/redaction requirements. |
| Cache leakage | Cached metric response is served across tenants. | Cache keys include tenant ID and query hash. |
| Poison message loop | Bad events repeatedly fail processing. | Retry topic and DLQ separation; replay only after fix and audit. |
| Noisy tenant | One tenant saturates shared infrastructure. | Tenant-scoped metrics, API usage logs, benchmark evidence, rate-limit scaffold, future dedicated isolation path. |

## Residual Risks

- Local JWT implementation is a scaffold and should be replaced with managed identity or central auth in production.
- Row-level security is documented but not enabled by default locally.
- Local Docker has single-node Kafka/Postgres/Redis and does not represent high-availability production posture.
