# ADR 0001: Split Kafka Topics by Event Domain

## Status

Accepted

## Context

The platform handles orders, payments, users, products, and system events. A single topic would simplify bootstrapping but would couple unrelated consumers and make retention, lag, and partition scaling harder to tune.

## Decision

Use domain-specific topics:

- `platform.events.orders`
- `platform.events.payments`
- `platform.events.users`
- `platform.events.products`
- `platform.events.system`
- `platform.events.retry`
- `platform.events.dlq`

## Consequences

Consumers subscribe only to contracts they own. High-volume user activity can scale independently from payment or system events. The tradeoff is more topic management, which is handled through `kafka/topics.yaml` and `kafka/create_topics.py`.

