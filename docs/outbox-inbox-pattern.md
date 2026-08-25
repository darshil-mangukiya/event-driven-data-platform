# Outbox and Inbox Pattern

The platform now includes schema and SQL plans for producer outbox and consumer inbox guarantees.

## Outbox

`event_outbox` stores events created transactionally by source systems before Kafka publish. A dispatcher leases pending rows using `for update skip locked`, publishes to Kafka, then marks rows as `published` or schedules retry.

Dry-run the dispatcher plan:

```bash
python scripts/outbox_dispatch_plan.py
```

Reference SQL lives in `sql/outbox/`.

## Inbox

`event_inbox` records events seen by the processing service before domain writes. If the same event is replayed, the `(consumer_name, event_id)` primary key blocks duplicate processing before metrics are incremented.

This is an exactly-once-style pattern built from idempotent writes, inbox deduplication, and replay-safe metric behavior. Kafka delivery is still at-least-once.

## Tradeoff

The local MVP stores outbox/inbox evidence in Postgres. Production producers should write outbox rows in the same transaction as source state changes and use a dispatcher with metrics for lease age, attempts, failures, and publish latency.
