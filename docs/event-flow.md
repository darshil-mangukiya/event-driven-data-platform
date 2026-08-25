# Event Flow

Every event uses the same envelope:

```json
{
  "event_id": "uuid",
  "tenant_id": "tenant_demo",
  "event_type": "order.created",
  "event_timestamp": "2026-01-01T00:00:00Z",
  "source_service": "ingestion-service",
  "payload_version": 1,
  "payload": {},
  "trace_id": "uuid"
}
```

## Topics

| Topic | Purpose | Partition key |
| --- | --- | --- |
| `platform.events.orders` | Order lifecycle events | `tenant_id:order_id` |
| `platform.events.payments` | Payment and risk events | `tenant_id:payment_id` |
| `platform.events.users` | Signup, activity, churn signals | `tenant_id:user_id` |
| `platform.events.products` | Catalog and inventory state | `tenant_id:product_id` |
| `platform.events.system` | Service health and platform events | `tenant_id:service_name` |
| `platform.events.retry` | Transient failures for replay | `tenant_id:original_event_id` |
| `platform.events.dlq` | Poison messages and audit failures | `tenant_id:original_event_id` |

## Ordering Assumptions

The platform assumes ordering only within a tenant plus business entity key. It does not assume total ordering across a tenant or topic. Consumers are therefore idempotent by natural keys such as `(tenant_id, order_id)` and `(tenant_id, payment_id)`.

## Retry and DLQ Strategy

The MVP validates schemas before publishing. Processing failures are separated into:

- Transient infrastructure failures, which should be retried through `platform.events.retry`.
- Poison messages, schema drift, and repeated processing failures, which go to `platform.events.dlq`.

DLQ records include the original event, failure stage, and error message so they can be replayed after a fix.

