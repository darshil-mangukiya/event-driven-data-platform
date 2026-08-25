# Integration Tests

`test_event_processor_flow.py` validates the in-process event flow without external infrastructure. It exercises the same `EventProcessor` used by the Kafka consumer and confirms that raw, processed, aggregate, and alert writes are triggered.

For a full local stack test, start Docker Compose and run:

```bash
python scripts/run_local_e2e.py
```

That script issues a JWT from the metadata service, posts an order event through ingestion, waits for the processing consumer, and queries the analytics API.

