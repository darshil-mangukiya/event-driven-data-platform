# ADR 0003: Start with Shared-Schema Multi-Tenancy

## Status

Accepted

## Context

The project simulates a fast-growing company with multiple internal tenants or business units. Early platform velocity matters, but tenant isolation must be explicit.

## Decision

Use shared infrastructure and shared tables with required `tenant_id`, tenant-scoped indexes, API access checks, and optional row-level-security policy scripts.

## Consequences

The design is cost-effective and easy to operate locally. The tradeoff is that every query and API must enforce tenant scope. Enterprise tenants can later move to schema, database, or infrastructure isolation.

