# Event Contracts

The platform uses JSON Schema files, examples, invalid fixtures, shared Pydantic models, and compatibility scripts to keep event behavior explicit.

## Envelope

Every event envelope includes:

- `event_id`
- `tenant_id`
- `event_type`
- `event_timestamp`
- `source_service`
- `payload_version`
- `trace_id`
- `correlation_id`
- `causation_id`
- `idempotency_key`
- `payload`

The envelope schema lives at `contracts/schemas/v1/event-envelope.schema.json`.

## Domain Contracts

| Domain | Example Contract Folder |
| --- | --- |
| Orders | `contracts/events/orders/` |
| Payments | `contracts/events/payments/` |
| Users | `contracts/events/users/` |
| Products | `contracts/events/products/` |
| System | `contracts/events/system/` |

Each domain includes a schema plus valid and invalid fixture payloads.

## Compatibility Rules

Default compatibility mode is backward compatibility.

Allowed:

- Add optional fields.
- Add new event types when consumers can ignore them safely.
- Add enum-like values only when consumers tolerate unknown values.

Not allowed without a migration:

- Remove a property.
- Remove a required field.
- Add a new required field.
- Change an existing field type.
- Move an event type to a different topic.

## Validation Commands

```bash
PYTHONPATH=services/shared python scripts/validate_event_contracts.py
PYTHONPATH=services/shared python scripts/check_contract_compatibility.py
```

## DLQ Behavior

Contract violations are rejected before normal processing where possible. Events that cannot be processed safely are routed or represented through DLQ tooling and replay audit records. Replay should happen only after the contract issue or downstream failure has been fixed and idempotency has been checked.

## Production Extension Path

A production deployment would normally add a managed schema registry or release-time contract gate. The local repo keeps the same discipline through checked-in schemas, fixtures, tests, and compatibility scripts.
