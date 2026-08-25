# Schema Registry Verification

Status: **VERIFIED**

Date: 2026-08-21

## Scope

The in-repository schema registry is a FastAPI/PostgreSQL service for the
project's JSON Schema contracts. It is not a Confluent-compatible wire
protocol and does not support Avro or Protobuf.

## Implemented behavior

- Five subjects bootstrap from `contracts/registry.json`.
- Registration checks BACKWARD, FORWARD, or FULL compatibility.
- Identical registration returns the current version without adding a row.
- Compatibility decisions are stored in
  `schema_registry_compatibility_checks`.
- Subject, version, compatibility, configuration, and bootstrap-status
  endpoints are exposed by `services/schema-registry-service`.

## Local runtime results

| Check | Result |
| --- | --- |
| PostgreSQL registry tables initialized | PASS |
| Service health endpoint | PASS |
| Five subjects registered at version 1 | PASS |
| Two compatible fixtures accepted | PASS |
| Two breaking fixtures rejected | PASS |
| Breaking registration returned HTTP 409 | PASS |
| Repeated bootstrap left versions unchanged | PASS |
| Integration test against the running service | PASS |

```bash
python scripts/validate_schema_registry.py --bootstrap --pretty
python scripts/validate_schema_registry.py --check-fixtures --pretty
python scripts/check_contract_compatibility.py
```

BACKWARD is the declared mode used by current subjects. FORWARD and FULL are
covered by unit tests.

## Boundary

The ingestion service does not query the registry before publishing. Contract
validation remains source-controlled and CI-enforced; runtime registration is
an independently callable governance service. Schema deletion and deprecation
endpoints are not implemented.
