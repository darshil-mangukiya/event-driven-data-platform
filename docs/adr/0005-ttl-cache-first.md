# ADR 0005: Use TTL-Based Redis Caching Before Event Invalidation

## Status

Accepted

## Context

Analytics endpoints serve repeated tenant metric queries. Cache invalidation can become complex if every aggregate update emits invalidation messages.

## Decision

Use stable tenant-scoped Redis keys and TTL-based expiration for local and MVP operation.

## Consequences

TTL caching is simple, safe, and easy to explain. Some data can be stale for the TTL window. A future production version can add event-driven invalidation using aggregate update notifications.

