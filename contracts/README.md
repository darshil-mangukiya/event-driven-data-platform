# Event Contract Registry

This folder provides a schema-registry-style contract layer without requiring a running Schema Registry locally.

Included:

- Versioned JSON Schema files under `schemas/v1/`.
- A backward-compatible order v2 example under `schemas/v2/`.
- `registry.json` mapping event subjects to event types, owners, and payload schemas.
- A compatibility policy in `compatibility.md`.
- Compatibility test cases under `compatibility_tests/`.
- Domain-level producer/consumer contracts and valid/invalid fixtures under `events/`.
- Validation scripts:

```bash
PYTHONPATH=services/shared python scripts/validate_event_contracts.py
PYTHONPATH=services/shared python scripts/check_contract_compatibility.py
```

The application still uses Pydantic for runtime validation. The contract registry gives reviewers a stable interface surface for event evolution, ownership, and compatibility discussions.

See also `contracts/data_products/` — a consumer-facing data product
registry that references these event types by name (never redefines them);
see [docs/data-products.md](../docs/data-products.md).
