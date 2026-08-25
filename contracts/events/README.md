# Domain Event Contracts

This folder gives each event domain a producer/consumer contract surface in addition to the shared registry in `contracts/registry.json`.

Each domain contains:

- a domain event schema
- a valid fixture
- an invalid fixture that should be rejected by producers or consumers

Compatibility expectations:

- Existing required fields cannot be removed.
- Field types cannot change without a new payload version.
- Enum values can be added only when consumers tolerate unknown values.
- Breaking changes require a new schema version and dual-publish or dual-read migration plan.
- Contract violations should be rejected at ingestion or routed to retry/DLQ handling with trace metadata.
