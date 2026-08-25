# ADR 0004: Use Spark for Backfills and Sessionization

## Status

Accepted

## Context

The processing service needs low-latency, idempotent event handling. Historical rebuilds, sessionization, and lakehouse compaction need larger distributed processing patterns.

## Decision

Use the processing service for near-real-time writes and aggregates. Use PySpark for streaming enrichment, event normalization, revenue aggregate rebuilds, sessionization, and lakehouse compaction.

## Consequences

Operational concerns are separated cleanly: APIs and consumers stay lean, while Spark handles heavy workloads. This adds another runtime, so docs and Docker Compose include Spark explicitly.

