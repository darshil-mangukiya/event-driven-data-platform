# Kafka End-to-End Result

Status: **EXECUTED AND VERIFIED**

- Event ID: `411eafe8-afca-4b0f-bcbf-9aef13c128db`
- Contract: `order.created`
- Kafka location: `platform.events.orders`, partition `0`, offset `0`
- Result: processing wrote the operational record and analytics returned gross
  revenue `98`, net revenue `95`, and order count `1` for the test date.
- Delivery guarantee: at-least-once with idempotent database effects.

This was the actual ingestion service, Kafka broker, processing service,
PostgreSQL, and analytics API under Docker Compose.
