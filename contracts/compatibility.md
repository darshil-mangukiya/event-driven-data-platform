# Event Compatibility Policy

Compatibility mode: `BACKWARD`.

Allowed changes:

- Add optional payload fields.
- Add new event types under a new or existing subject when consumers can ignore them.
- Add enum-like values only when downstream consumers treat unknown values safely.
- Increase payload version when behavior changes materially.

Breaking changes:

- Removing required fields.
- Renaming fields.
- Changing field types.
- Changing event ordering assumptions.
- Moving an event type to a different topic without a migration window.

Migration rules:

1. Add optional fields first.
2. Deploy consumers that can read both old and new payloads.
3. Increase `payload_version`.
4. Backfill or replay only after idempotency review.
5. Document the migration in an ADR when it changes platform behavior.

Validation:

```bash
PYTHONPATH=services/shared python scripts/check_contract_compatibility.py
```

The current compatibility test proves that `schemas/v2/order-payload.schema.json` only adds optional fields to the v1 order payload.
