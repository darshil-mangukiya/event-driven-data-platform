# Compatibility Test Fixtures

These schemas are **fixtures only** — synthetic old/new schema pairs
constructed specifically to exercise each compatibility outcome the Schema
Registry's compatibility checker (`platform_shared.schema_compatibility`)
must classify correctly. They are not used by any running service and are
never registered as real event contracts. Real contract compatibility
(the actual `order-payload` v1→v2 evolution) is covered separately by
`contracts/compatibility_tests/order_v1_to_v2.json`, which points at the
genuine `contracts/schemas/v1`/`v2` files.

| Fixture pair | Outcome | What it tests |
| --- | --- | --- |
| `compatible_optional_field_add_old.schema.json` / `_new.schema.json` | compatible | adding a new optional field |
| `compatible_safe_change_old.schema.json` / `_new.schema.json` | compatible | widening `additionalProperties` and leaving all existing fields/types untouched |
| `breaking_required_field_old.schema.json` / `_new.schema.json` | breaking | a new field is added as `required` — old producers that don't send it would now fail validation |
| `breaking_type_change_old.schema.json` / `_new.schema.json` | breaking | an existing field's `type` changes (`string` → `integer`) — old data would fail validation against the new schema |
