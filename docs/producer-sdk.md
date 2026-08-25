# Producer SDK

The Python producer SDK lives in `sdk/python/`.

It gives upstream applications a safer way to publish events:

- validates payloads with the shared platform contracts
- derives deterministic `event_id` values from idempotency keys
- retries transient HTTP failures with simple backoff
- supports single-event and batch publish flows

Run the example with the ingestion service running:

```bash
PYTHONPATH=services/shared:sdk/python python sdk/python/examples/order_producer.py
```

Production additions would include service identity, request tracing propagation, producer metrics, and package publishing to an internal registry.
