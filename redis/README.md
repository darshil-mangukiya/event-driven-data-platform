# Redis Cache Layer

Redis accelerates hot analytics API responses and stores lightweight rate-limit counters.

Caching policy:

- Tenant-scoped metric endpoints use stable keys shaped as `metrics:<metric>:tenant:<tenant_id>:<query_hash>`.
- Default TTL is 120 seconds locally. Production TTLs should be metric-specific: shorter for revenue and alerts, longer for product catalog summaries.
- Cache invalidation can be event-driven by publishing aggregate-update notifications after processing commits. The local MVP uses TTL because it is safer and easier to reason about without a distributed invalidation service.
- Rate limit keys expire every 60 seconds and are scoped by tenant, user, and endpoint.

