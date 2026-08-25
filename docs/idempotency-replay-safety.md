# Idempotency and Replay Safety

The platform uses two layers of replay protection:

| Layer | Mechanism |
| --- | --- |
| Ingestion | Clients can send `idempotency_key`; the ingestion service derives a stable `event_id` from tenant, event type, source service, and key. |
| Processing | `raw_events.event_id` is the first write. If the event already exists, processing skips domain writes and metric deltas. |

## Why This Matters

Kafka consumers may retry messages. DLQ replay can send the same event back through the processor. Clients may also retry HTTP ingestion after a timeout even when Kafka publish succeeded.

Stable event IDs plus a raw-event uniqueness check keep those retries from double-counting daily metrics.

## Client Contract

For business events, producers should provide one stable idempotency key per source business transition:

```json
{
  "tenant_id": "tenant_demo",
  "event_type": "order.created",
  "source_service": "checkout-api",
  "idempotency_key": "checkout-ord-1001-created",
  "payload": {
    "order_id": "ord_1001",
    "customer_id": "cust_1001",
    "product_id": "prod_001",
    "quantity": 2,
    "unit_price": 49.0
  }
}
```

If a source system already has immutable event IDs, it may send `event_id` directly. The platform treats that as the replay key.

## Current MVP Boundary

The MVP deduplicates identical event IDs at processing time. A production version should also persist idempotency-key request fingerprints at ingestion time so the API can reject a repeated idempotency key with a different payload before Kafka publish.

## Replay Checklist

1. Confirm whether replay source is Kafka retry, DLQ, or batch backfill.
2. Confirm source events carry stable `event_id` or `idempotency_key`.
3. Replay into the domain topic or retry topic.
4. Watch `raw_events` insert counts and metric deltas.
5. Run `scripts/reconcile_metrics.py` for affected tenant/date ranges.
6. Flush or wait out Redis metric TTL if the correction is urgent.
