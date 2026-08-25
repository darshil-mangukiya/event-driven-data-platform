# ADR 0007: In-Repo Schema Registry and Generated AsyncAPI, Not Confluent Schema Registry

## Status

Accepted

## Context

The platform already had static JSON Schema contracts under `contracts/`
and a compatibility-checking script
(`scripts/check_contract_compatibility.py`). the corresponding verification needed real
subject/version/compatibility-mode governance backed by a live service,
plus a machine-readable event-architecture spec (AsyncAPI), without
introducing a paid or heavyweight dependency (Confluent Schema Registry or
a hosted registry SaaS) for the local deployment.

## Decision

Build a small, real FastAPI service (`schema-registry-service`) backed by
PostgreSQL, storing subjects/versions/compatibility-check history in the
same database the rest of the platform already uses. Extract the
compatibility algorithms (`compare_backward_compatible`,
`compare_forward_compatible`, `compare_full_compatible`) into
`platform_shared.schema_compatibility` so both the CLI script and the
service use one implementation. Generate `contracts/asyncapi.yml` from
the live `TOPIC_DEFINITIONS` + `contracts/registry.json` rather than
hand-authoring it, so the spec cannot drift from the real topics.

## Consequences

No new infrastructure dependency (no Confluent Schema Registry, no
external SaaS); the registry's data model and audit trail are readable
via ordinary SQL like the rest of the platform. The tradeoff is that this
registry does not have the ecosystem tooling (client libraries, IDE
plugins) that Confluent Schema Registry has — acceptable, since the
system's actual compatibility enforcement (CI's
`check_contract_compatibility.py` gate) does not depend on which registry
implementation stores the history. AsyncAPI generation being derived
(not hand-written) means it is regenerated, not manually kept in sync,
whenever a topic changes.
