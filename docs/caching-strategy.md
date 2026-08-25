# Caching Strategy

Redis caches repeated metric responses and supports lightweight rate-limit state.

## Cached Endpoints

- `/metrics/revenue`
- `/metrics/customers`
- `/metrics/churn`
- `/metrics/retention`
- `/metrics/marketing_roi`
- `/metrics/product_performance`

## TTL Strategy

Local default: 120 seconds.

Recommended production defaults:

- Revenue and customers: 60 to 180 seconds.
- Product performance: 5 to 10 minutes.
- Marketing ROI: 5 to 15 minutes unless campaign spend is updated more frequently.
- Alerts and status: avoid long caching or use very short TTLs.

## Invalidation

The MVP uses TTL-based invalidation. Production can add event-driven invalidation when aggregate tables are updated, using a small `metrics.updated` topic or Redis pub/sub.

