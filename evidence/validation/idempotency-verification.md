# Idempotency / At-Least-Once Delivery Verification

Scope: Local verification
Date: 2026-08-22

## Status: LOCALLY VERIFIED

The delivery model is **at-least-once with idempotent consumers**.
Exactly-once processing is outside this scope.

## Experiment

The same logical event (`idempotency_key:
idempotency-test-key-DUPLICATE`, `order_id: ord_idempotency_dup_test`)
was published **100 times** in sequence against the running
`ingestion-service` (Docker Compose), each a separate HTTP
POST/publish attempt (not a client-side retry-collapse) — simulating
100 delivery attempts of the same business event, as would happen with a
naive at-least-once retry policy.

```
$ for i in $(seq 1 100); do
    curl -X POST http://localhost:8001/events \
      -H "Authorization: Bearer <real JWT>" \
      -d '{ ... "idempotency_key": "idempotency-test-key-DUPLICATE",
            "order_id": "ord_idempotency_dup_test", "quantity": 5,
            "unit_price": 99.0, ... }'
  done
```

## Measured

| Measurement | Value |
|---|---|
| Publish attempts | 100 |
| Kafka offsets returned | **100 distinct offsets** (0–99) |
| Distinct `event_id` values returned | **1** (deterministically derived from `idempotency_key`) |
| Rows in `raw_events` for this order | **1** |
| Rows in `processed_orders` for this order | **1**, with the correct final values (`quantity: 5, unit_price: 99.00, status: created`) |

## Interpretation

- **100 distinct Kafka offsets** proves Kafka itself performed **no**
  deduplication at publish/broker time; each attempt was delivered.
  Transport semantics are at-least-once.
- **1 distinct `event_id`** across all 100 attempts shows the
  ingestion-service deterministically derives the event identity from
  the idempotency key, not from a random/attempt-specific value — the
  precondition idempotent processing depends on.
- **1 final row**, not 100, in both `raw_events` and `processed_orders`
  confirms duplicate collapse in durable serving state.

## Command

```
$ PGPASSWORD=*** psql -h 127.0.0.1 -p 15432 -U platform -d data_platform -c \
    "select order_id, tenant_id, quantity, unit_price, status from processed_orders \
     where order_id = 'ord_idempotency_dup_test';"

         order_id         |  tenant_id  | quantity | unit_price | status
--------------------------+-------------+----------+------------+---------
 ord_idempotency_dup_test | tenant_demo |        5 |      99.00 | created
```

## Limitations

- 100 sequential attempts from a single client; not a concurrent-writer
  race-condition stress test (that would be a separate, harder
  experiment — concurrent upserts racing on the same idempotency key —
  not attempted verification).
- Single tenant, single order — not a multi-tenant duplicate-isolation
  test (tenant isolation itself is already covered by RLS's own live
  test matrix).
- Does not test duplicate delivery *across* a consumer restart/rebalance
  (that scenario is already covered by `reliability/scenarios/duplicate_event.py`
  and its regression test).

## Result

One logical event produced 100 distinct Kafka offsets and one final row in
each durable table. Idempotent processing supplies the durability guarantee.
