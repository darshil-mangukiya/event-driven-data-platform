# Data Product Catalog

Generated from `contracts/data_products/registry.yml`. Do not hand-edit —
regenerate with `python scripts/generate_data_product_catalog.py` or
`make data-products-catalog`.

These are modeled internal data products used to document the platform design.
See [docs/consumer-requirements.md](../../docs/consumer-requirements.md)
for the modeled-vs-real distinction.

| Product | Consumers | Metrics | API | Freshness | SLO | Quality | Tenant Scope | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Revenue & Order Metrics | Finance | revenue | `/metrics/revenue` | 15 minutes (docs/metric-sla.md) | Serving Freshness (Async Path); Reconciliation Correctness | raw_event_required_fields, processed_order_event_uniqueness, processed_order_revenue_ranges | Yes | active |
| Payment Health | Finance, Risk | payment_success, tenant_health_score | `/metrics/payment_success` | 15 minutes (near-real-time aggregate updates, docs/metric-sla.md revenue analog) | Serving Freshness (Async Path) | raw_event_required_fields | Yes | active |
| Customer Activity | Product | customers, churn, retention | `/metrics/customers` | 30 minutes (docs/metric-sla.md) | Serving Freshness (Async Path) | raw_event_required_fields | Yes | active |
| Product Performance | Product | product_performance | `/metrics/product_performance` | 60 minutes (docs/metric-sla.md) | Serving Freshness (Async Path) | processed_order_revenue_ranges | Yes | active |
| Marketing ROI Proxy | Marketing | marketing_roi | `/metrics/marketing_roi` | 60 minutes (docs/metric-sla.md) | Serving Freshness (Async Path) | processed_order_revenue_ranges | Yes | active |
| Platform Health | Operations, Risk | event_throughput, tenant_health_score | `/metrics/tenant_health_score` | 5 minutes (docs/metric-sla.md /system/status analog) | Streaming Availability; Processing Lag; DLQ Rate; Checkpoint Freshness | raw_event_freshness | Yes | active |

## Product Detail

### Revenue & Order Metrics (`revenue`)

- **Domain**: finance
- **Owner (modeled)**: finance-analytics
- **Description**: Tenant-scoped gross/net revenue, order count, units sold, and average order value, derived from processed order events.
- **Modeled consumers**: finance
- **Business questions**:
  - What is revenue for a tenant and period?
  - How fresh is the result?
  - Does serving revenue reconcile to processed orders?
- **Source events**: order.created, order.updated
- **Serving table**: `tenant_metrics_daily`
- **API endpoint**: `/metrics/revenue`
- **Grain**: tenant_id, metric_date
- **Measures**: gross_revenue, net_revenue, order_count, units_sold, average_order_value
- **Freshness target**: 15 minutes (docs/metric-sla.md)
- **Latency target**: p95 < 300ms cached / < 750ms uncached (docs/slo-catalog.md #5)
- **SLO reference**: Serving Freshness (Async Path), Reconciliation Correctness
- **Tenant scoped**: True
- **Isolation rule**: Authenticated tenant may retrieve only its own records — enforced by assert_tenant_scope() against the JWT/header-derived TenantPrincipal (services/analytics-service/app/main.py), the SQL WHERE tenant_id = $1 filter in AnalyticsRepository.revenue(), and covered by tests/test_tenant_access_and_cache.py::test_tenant_principal_blocks_cross_tenant_access.
- **Lineage**:
  - order.created/order.updated -> Kafka platform.events.orders -> processing-service -> processed_orders -> tenant_metrics_hourly -> tenant_metrics_daily -> /metrics/revenue -> Finance consumer
  - (parallel) order.created/order.updated -> Kafka -> Spark Structured Streaming (validate/dedup/watermark/window) -> stream_window_metrics [not yet API-served]
- **Cache**: enabled=True, ttl=120s, fallback: Redis unavailable -> cache miss -> loader queries PostgreSQL directly; request still succeeds, just uncached (RedisCache.get_json returns None on outage).
- **Failure behavior**:
  - Redis unavailable -> falls back to PostgreSQL per cache.fallback_behavior above; platform_cache_available{cache=redis} drops to 0 (RedisUnavailable alert).
  - PostgreSQL unavailable -> request fails with a controlled 5xx (Postgres.execute propagates); no silent empty/zero response.
  - Streaming/serving freshness SLO violated -> cloudscale_serving_metrics_staleness_seconds{table=tenant_metrics_daily} rises past 10800s -> ServingMetricsStale alert (not a silent-healthy state).
- **Acceptance criteria**:
  - Revenue responses must be tenant scoped.
  - Serving revenue must reconcile against the authoritative processed source within the configured tolerance (0.01).
  - A stale data product must surface a health/freshness signal rather than appearing silently healthy.
  - Redis unavailability must follow the documented fallback behavior.
  - Unknown tenants must not receive another tenant's metrics.
- **Known limitations**:
  - Local initialization values are for development workflows and are not financial evidence.
  - stream_window_metrics (the Structured Streaming aggregation) is not yet served by this API — see docs/data-products.md Limitations.

### Payment Health (`payment_health`)

- **Domain**: finance
- **Owner (modeled)**: finance-risk
- **Description**: Tenant-scoped payment success/failure counts and success rate, plus the tenant health score's payment-failure input.
- **Modeled consumers**: finance, risk
- **Business questions**:
  - What is the payment success/failure rate for a tenant?
  - Are payment failures indicating abnormal behavior?
- **Source events**: payment.authorized, payment.captured, payment.failed
- **Serving table**: `tenant_metrics_daily`
- **API endpoint**: `/metrics/payment_success`
- **Grain**: tenant_id, metric_date
- **Measures**: payment_success_count, payment_failure_count, payment_success_rate
- **Freshness target**: 15 minutes (near-real-time aggregate updates, docs/metric-sla.md revenue analog)
- **Latency target**: p95 < 750ms uncached (docs/slo-catalog.md #5)
- **SLO reference**: Serving Freshness (Async Path)
- **Tenant scoped**: True
- **Isolation rule**: Authenticated tenant may retrieve only its own records — same assert_tenant_scope() + SQL tenant_id filter mechanism as revenue; see tests/test_tenant_access_and_cache.py.
- **Lineage**:
  - payment.authorized/captured/failed -> Kafka platform.events.payments -> processing-service -> processed_payments -> tenant_metrics_daily -> /metrics/payment_success -> Finance/Risk consumer
  - processed_payments (high-risk failure) -> alerts -> Ops Console / demo dashboard
- **Cache**: enabled=True, ttl=120s, fallback: Redis unavailable -> cache miss -> PostgreSQL fallback, same mechanism as revenue.
- **Failure behavior**:
  - Redis unavailable -> PostgreSQL fallback (see cache.fallback_behavior).
  - PostgreSQL unavailable -> request fails with a controlled 5xx.
- **Acceptance criteria**:
  - Payment health responses must be tenant scoped.
  - Redis unavailability must follow the documented fallback behavior.
  - Unknown tenants must not receive another tenant's metrics.
- **Known limitations**:
  - Local data does not model all processor states such as chargebacks or refunds.

### Customer Activity (`customer_activity`)

- **Domain**: product
- **Owner (modeled)**: product-analytics
- **Description**: Tenant-scoped new/active user counts, churn-signal counts, and a retention proxy derived from user session events.
- **Modeled consumers**: product
- **Business questions**:
  - Which products are performing well?
  - How is customer/product activity changing?
- **Source events**: user.signed_up, user.activity, user.churn_signal
- **Serving table**: `tenant_metrics_daily`
- **API endpoint**: `/metrics/customers`
- **Grain**: tenant_id, metric_date
- **Measures**: new_users, active_users, churn_signal_count, retention_rate
- **Freshness target**: 30 minutes (docs/metric-sla.md)
- **Latency target**: p95 < 750ms uncached (docs/slo-catalog.md #5)
- **SLO reference**: Serving Freshness (Async Path)
- **Tenant scoped**: True
- **Isolation rule**: Authenticated tenant may retrieve only its own records — same assert_tenant_scope() + SQL tenant_id filter mechanism as revenue.
- **Lineage**:
  - user.signed_up/activity/churn_signal -> Kafka platform.events.users -> processing-service -> processed_user_sessions -> tenant_metrics_daily -> /metrics/customers, /metrics/churn, /metrics/retention -> Product consumer
- **Cache**: enabled=True, ttl=120s, fallback: Redis unavailable -> cache miss -> PostgreSQL fallback.
- **Failure behavior**:
  - Redis unavailable -> PostgreSQL fallback.
  - PostgreSQL unavailable -> request fails with a controlled 5xx.
- **Acceptance criteria**:
  - Customer activity responses must be tenant scoped.
  - Redis unavailability must follow the documented fallback behavior.
  - Churn and retention must remain labeled as signals/proxies, not confirmed churn or contractual retention.
- **Known limitations**:
  - Churn is a signal metric (count of churn_signal/cancel_intent events), not confirmed contractual churn — see docs/metric-definitions.md Proxy Metrics.
  - Retention is a proxy (1 - churn_signal_count / active_users), not a cohort-based retention calculation — true retention requires a longer-lived identity/subscription model.
  - Anonymous events require identity stitching before production use.

### Product Performance (`product_performance`)

- **Domain**: product
- **Owner (modeled)**: product-analytics
- **Description**: Product-level revenue and units sold by tenant, joined against product metadata.
- **Modeled consumers**: product
- **Business questions**:
  - Which products generate the most activity or revenue?
- **Source events**: order.created, product.upserted, product.inventory_changed
- **Serving table**: `processed_orders`
- **API endpoint**: `/metrics/product_performance`
- **Grain**: tenant_id, product_id
- **Measures**: net_revenue, units_sold
- **Freshness target**: 60 minutes (docs/metric-sla.md)
- **Latency target**: p95 < 750ms uncached (docs/slo-catalog.md #5)
- **SLO reference**: Serving Freshness (Async Path)
- **Tenant scoped**: True
- **Isolation rule**: Authenticated tenant may retrieve only its own records — same assert_tenant_scope() + SQL tenant_id filter mechanism as revenue.
- **Lineage**:
  - order.created -> Kafka platform.events.orders -> processing-service -> processed_orders (joined with tenant_products) -> /metrics/product_performance -> Product consumer
- **Cache**: enabled=True, ttl=120s, fallback: Redis unavailable -> cache miss -> PostgreSQL fallback.
- **Failure behavior**:
  - Redis unavailable -> PostgreSQL fallback.
  - PostgreSQL unavailable -> request fails with a controlled 5xx.
- **Acceptance criteria**:
  - Product performance responses must be tenant scoped.
  - Redis unavailability must follow the documented fallback behavior.
- **Known limitations**:
  - Inventory state is event-derived and should be reconciled with source-of-truth catalog systems.

### Marketing ROI Proxy (`marketing_performance`)

- **Domain**: marketing
- **Owner (modeled)**: marketing-analytics
- **Description**: Tenant campaign return based on attributed revenue and local development campaign spend — a proxy, not a production attribution model.
- **Modeled consumers**: marketing
- **Business questions**:
  - What marketing-performance proxy is available from the modeled event data?
- **Source events**: order.created
- **Serving table**: `tenant_metrics_daily`
- **API endpoint**: `/metrics/marketing_roi`
- **Grain**: tenant_id, metric_date
- **Measures**: marketing_spend, marketing_attributed_revenue, marketing_roi
- **Freshness target**: 60 minutes (docs/metric-sla.md)
- **Latency target**: p99.0% monthly availability (docs/metric-sla.md)
- **SLO reference**: Serving Freshness (Async Path)
- **Tenant scoped**: True
- **Isolation rule**: Authenticated tenant may retrieve only its own records — same assert_tenant_scope() + SQL tenant_id filter mechanism as revenue.
- **Lineage**:
  - order.created (with marketing_campaign_id) -> Kafka platform.events.orders -> processing-service -> processed_orders -> tenant_metrics_daily -> /metrics/marketing_roi -> Marketing consumer
- **Cache**: enabled=True, ttl=120s, fallback: Redis unavailable -> cache miss -> PostgreSQL fallback.
- **Failure behavior**:
  - Redis unavailable -> PostgreSQL fallback.
  - PostgreSQL unavailable -> request fails with a controlled 5xx.
- **Acceptance criteria**:
  - Marketing ROI responses must be tenant scoped.
  - This product must remain labeled a proxy, not a production attribution model.
- **Known limitations**:
  - Local MVP uses simplified spend attribution; production would join real ad-platform spend feeds.
  - This is explicitly a proxy metric — see docs/metric-definitions.md Proxy Metrics for what it does and does not represent.

### Platform Health (`platform_health`)

- **Domain**: operations
- **Owner (modeled)**: data-platform
- **Description**: Composite operational health signal (tenant_health_score), event throughput, and active alerts — the operations/risk view of whether the platform itself is behaving normally.
- **Modeled consumers**: operations, risk
- **Business questions**:
  - Is event processing healthy?
  - Are data products fresh?
  - Is processing lag increasing?
  - Are failures accumulating in the DLQ?
  - Which alerts require review?
- **Source events**: order.created, payment.authorized, payment.captured, payment.failed, user.activity, system.health, system.alert
- **Serving table**: `tenant_metrics_daily`
- **API endpoint**: `/metrics/tenant_health_score`
- **Grain**: tenant_id
- **Measures**: tenant_health_score, events_processed
- **Freshness target**: 5 minutes (docs/metric-sla.md /system/status analog)
- **Latency target**: p99.9% monthly availability (docs/metric-sla.md)
- **SLO reference**: Streaming Availability, Processing Lag, DLQ Rate, Checkpoint Freshness
- **Tenant scoped**: True
- **Isolation rule**: Authenticated tenant may retrieve only its own record — same assert_tenant_scope() mechanism as revenue; /alerts and /system/status apply the same principal-based scoping.
- **Lineage**:
  - system.health/system.alert -> Kafka platform.events.system -> service_health_metrics / alerts -> /metrics/tenant_health_score, /alerts -> Operations/Risk consumer
  - (streaming path) Kafka domain topics -> Spark Structured Streaming -> cloudscale_stream_* Prometheus metrics -> Grafana / alert rules (see docs/OBSERVABILITY.md)
- **Cache**: enabled=False, ttl=0s, fallback: n/a — no cache in the request path for this endpoint.
- **Failure behavior**:
  - PostgreSQL unavailable -> request fails with a controlled 5xx.
  - Streaming checkpoint stale -> cloudscale_stream_checkpoint_age_seconds alert (StreamingCheckpointStale) fires rather than the pipeline silently going quiet.
  - Reliability exercise failure -> cloudscale_reliability_exercise_last_status drops to 0, surfaced on the Grafana Reliability Exercise Results panel.
- **Acceptance criteria**:
  - Platform health responses must be tenant scoped.
  - A stale data product must surface a health/freshness signal rather than appearing silently healthy.
  - Score weights are documented as MVP defaults, not calibrated production thresholds.
- **Known limitations**:
  - Score weights are MVP defaults and should be calibrated against real incidents.
  - Kafka lag and exact offset accounting require live broker telemetry not available in this local environment.
