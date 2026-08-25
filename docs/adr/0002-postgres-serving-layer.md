# ADR 0002: Use PostgreSQL as the Analytics Serving Layer

## Status

Accepted

## Context

The platform needs tenant-scoped analytics APIs with predictable latency, SQL-friendly metrics, and easy local deployment.

## Decision

Use PostgreSQL for raw audit tables, processed domain tables, serving aggregates, alerts, and monitoring state.

## Consequences

Postgres gives strong SQL ergonomics and simple local operations. It is not the long-term raw event archive for very high volume; MinIO/S3 parquet handles replayable history, and Spark handles historical rebuilds.

