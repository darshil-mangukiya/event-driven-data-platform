# Requirements Traceability Report

Generated from `contracts/data_products/requirements.yml`. Do not
hand-edit — regenerate with `python scripts/generate_data_product_catalog.py`
or `make requirements-trace`.

Total requirements: 11

| Requirement | Consumer | Business Question | Product | Metric | Source Event | Serving Table | API | Validation | SLO | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| FIN-001 | Finance | What is tenant revenue for a given period? | Revenue & Order Metrics | revenue | `order.created` | `tenant_metrics_daily` | `/metrics/revenue` | processed_order_revenue_ranges | Serving Freshness (Async Path) | active |
| FIN-002 | Finance | Does serving revenue reconcile to processed orders? | Revenue & Order Metrics | revenue | `order.created` | `tenant_metrics_daily` | `/metrics/revenue` | reconciliation_revenue_tolerance | Reconciliation Correctness | active |
| FIN-003 | Finance | What is the payment success/failure rate for a tenant? | Payment Health | payment_success | `payment.failed` | `tenant_metrics_daily` | `/metrics/payment_success` | raw_event_required_fields | Serving Freshness (Async Path) | active |
| PROD-001 | Product | How is customer/product activity changing? | Customer Activity | customers | `user.activity` | `tenant_metrics_daily` | `/metrics/customers` | raw_event_required_fields | Serving Freshness (Async Path) | active |
| PROD-002 | Product | Which products generate the most activity or revenue? | Product Performance | product_performance | `order.created` | `processed_orders` | `/metrics/product_performance` | processed_order_revenue_ranges | Serving Freshness (Async Path) | active |
| MKT-001 | Marketing | What marketing-performance proxy is available from the modeled event data? | Marketing ROI Proxy | marketing_roi | `order.created` | `tenant_metrics_daily` | `/metrics/marketing_roi` | processed_order_revenue_ranges | Serving Freshness (Async Path) | active |
| OPS-001 | Operations | Is event processing healthy? | Platform Health | tenant_health_score | `system.health` | `tenant_metrics_daily` | `/metrics/tenant_health_score` | raw_event_freshness | Streaming Availability | active |
| OPS-002 | Operations | Is processing lag increasing, and are failures accumulating in the DLQ? | Platform Health | event_throughput | `system.health` | `streaming_checkpoint_audit` | `/metrics/event_throughput` | cloudscale_stream_dlq_total | Processing Lag | active |
| RISK-001 | Risk | Are payment failures or platform signals indicating abnormal behavior? | Payment Health | payment_success | `payment.failed` | `alerts` | `/alerts` | raw_event_required_fields | Serving Freshness (Async Path) | active |
| RISK-002 | Risk | Which alerts require review? | Platform Health | tenant_health_score | `system.alert` | `alerts` | `/alerts` | raw_event_required_fields | DLQ Rate | active |
| OPS-003 | Operations | Are data products fresh? | Platform Health | tenant_health_score | `system.health` | `tenant_metrics_daily` | `/metrics/tenant_health_score` | raw_event_freshness | Event-Processing Freshness | active |

## Full Trace Detail

### FIN-001: What is tenant revenue for a given period?

```text
FIN-001
  -> Consumer: Finance
  -> Business Question: What is tenant revenue for a given period?
  -> Data Product: Revenue & Order Metrics (revenue)
  -> Metric: revenue
  -> Source Event: order.created
  -> Processing: processing-service order handler -> processed_orders -> tenant_metrics_hourly -> tenant_metrics_daily rollup
  -> Serving Table: tenant_metrics_daily
  -> API Endpoint: /metrics/revenue
  -> Validation Rule: processed_order_revenue_ranges
  -> SLO: Serving Freshness (Async Path)
  -> Test Reference: tests/test_tenant_access_and_cache.py::test_tenant_principal_blocks_cross_tenant_access
```

Acceptance criteria:
- Revenue responses must be tenant scoped.
- API value must reconcile within the configured tolerance (0.01) against processed_orders.

### FIN-002: Does serving revenue reconcile to processed orders?

```text
FIN-002
  -> Consumer: Finance
  -> Business Question: Does serving revenue reconcile to processed orders?
  -> Data Product: Revenue & Order Metrics (revenue)
  -> Metric: revenue
  -> Source Event: order.created
  -> Processing: scripts/reconcile_metrics.py::evaluate_reconciliation compares tenant_metrics_daily against a fresh aggregate over processed_orders
  -> Serving Table: tenant_metrics_daily
  -> API Endpoint: /metrics/revenue
  -> Validation Rule: reconciliation_revenue_tolerance
  -> SLO: Reconciliation Correctness
  -> Test Reference: tests/test_reliability_governance_tooling.py::test_reconciliation_detects_metric_drift
```

Acceptance criteria:
- Serving revenue must reconcile against the authoritative processed source within the configured tolerance.
- A reconciliation mismatch must be flagged as failed with exact deltas, not silently ignored.

### FIN-003: What is the payment success/failure rate for a tenant?

```text
FIN-003
  -> Consumer: Finance
  -> Business Question: What is the payment success/failure rate for a tenant?
  -> Data Product: Payment Health (payment_health)
  -> Metric: payment_success
  -> Source Event: payment.failed
  -> Processing: processing-service payment handler -> processed_payments -> tenant_metrics_daily rollup
  -> Serving Table: tenant_metrics_daily
  -> API Endpoint: /metrics/payment_success
  -> Validation Rule: raw_event_required_fields
  -> SLO: Serving Freshness (Async Path)
  -> Test Reference: tests/test_processing_logic.py::test_high_risk_failed_payment_creates_alert_signal
```

Acceptance criteria:
- Payment health responses must be tenant scoped.

### PROD-001: How is customer/product activity changing?

```text
PROD-001
  -> Consumer: Product
  -> Business Question: How is customer/product activity changing?
  -> Data Product: Customer Activity (customer_activity)
  -> Metric: customers
  -> Source Event: user.activity
  -> Processing: processing-service user handler -> processed_user_sessions -> tenant_metrics_daily rollup
  -> Serving Table: tenant_metrics_daily
  -> API Endpoint: /metrics/customers
  -> Validation Rule: raw_event_required_fields
  -> SLO: Serving Freshness (Async Path)
  -> Test Reference: tests/test_tenant_access_and_cache.py::test_cache_key_is_stable_and_tenant_scoped
```

Acceptance criteria:
- Customer activity responses must be tenant scoped.

### PROD-002: Which products generate the most activity or revenue?

```text
PROD-002
  -> Consumer: Product
  -> Business Question: Which products generate the most activity or revenue?
  -> Data Product: Product Performance (product_performance)
  -> Metric: product_performance
  -> Source Event: order.created
  -> Processing: query-time join of processed_orders against tenant_products metadata
  -> Serving Table: processed_orders
  -> API Endpoint: /metrics/product_performance
  -> Validation Rule: processed_order_revenue_ranges
  -> SLO: Serving Freshness (Async Path)
  -> Test Reference: tests/test_tenant_access_and_cache.py::test_tenant_principal_blocks_cross_tenant_access
```

Acceptance criteria:
- Product performance responses must be tenant scoped.

### MKT-001: What marketing-performance proxy is available from the modeled event data?

```text
MKT-001
  -> Consumer: Marketing
  -> Business Question: What marketing-performance proxy is available from the modeled event data?
  -> Data Product: Marketing ROI Proxy (marketing_performance)
  -> Metric: marketing_roi
  -> Source Event: order.created
  -> Processing: processed_orders with marketing_campaign_id -> tenant_metrics_daily rollup
  -> Serving Table: tenant_metrics_daily
  -> API Endpoint: /metrics/marketing_roi
  -> Validation Rule: processed_order_revenue_ranges
  -> SLO: Serving Freshness (Async Path)
  -> Test Reference: tests/test_tenant_access_and_cache.py::test_tenant_principal_blocks_cross_tenant_access
```

Acceptance criteria:
- Marketing ROI responses must be tenant scoped.
- This product must remain labeled a proxy, not a production attribution model.

### OPS-001: Is event processing healthy?

```text
OPS-001
  -> Consumer: Operations
  -> Business Question: Is event processing healthy?
  -> Data Product: Platform Health (platform_health)
  -> Metric: tenant_health_score
  -> Source Event: system.health
  -> Processing: query-time composite over tenant_metrics_daily, service_health_metrics, and alerts
  -> Serving Table: tenant_metrics_daily
  -> API Endpoint: /metrics/tenant_health_score
  -> Validation Rule: raw_event_freshness
  -> SLO: Streaming Availability
  -> Test Reference: tests/test_observability.py::test_alert_rules_streaming_group_exists
```

Acceptance criteria:
- A stale data product must surface a health/freshness signal rather than appearing silently healthy.

### OPS-002: Is processing lag increasing, and are failures accumulating in the DLQ?

```text
OPS-002
  -> Consumer: Operations
  -> Business Question: Is processing lag increasing, and are failures accumulating in the DLQ?
  -> Data Product: Platform Health (platform_health)
  -> Metric: event_throughput
  -> Source Event: system.health
  -> Processing: Spark Structured Streaming query.lastProgress -> cloudscale_stream_processing_lag_seconds / cloudscale_stream_dlq_total
  -> Serving Table: streaming_checkpoint_audit
  -> API Endpoint: /metrics/event_throughput
  -> Validation Rule: cloudscale_stream_dlq_total
  -> SLO: Processing Lag
  -> Test Reference: tests/test_observability.py::test_grafana_dashboard_has_streaming_row
```

Acceptance criteria:
- Processing lag and DLQ growth must be observable via Prometheus, not only inferable after the fact.

### RISK-001: Are payment failures or platform signals indicating abnormal behavior?

```text
RISK-001
  -> Consumer: Risk
  -> Business Question: Are payment failures or platform signals indicating abnormal behavior?
  -> Data Product: Payment Health (payment_health)
  -> Metric: payment_success
  -> Source Event: payment.failed
  -> Processing: processing-service payment handler flags high-risk failed payments as an alert signal
  -> Serving Table: alerts
  -> API Endpoint: /alerts
  -> Validation Rule: raw_event_required_fields
  -> SLO: Serving Freshness (Async Path)
  -> Test Reference: tests/test_processing_logic.py::test_high_risk_failed_payment_creates_alert_signal
```

Acceptance criteria:
- High-risk failed payments must produce an alert signal, not be silently recorded.

### RISK-002: Which alerts require review?

```text
RISK-002
  -> Consumer: Risk
  -> Business Question: Which alerts require review?
  -> Data Product: Platform Health (platform_health)
  -> Metric: tenant_health_score
  -> Source Event: system.alert
  -> Processing: alerts table populated by processing-service and reliability/observability signals; surfaced via ops-console and /alerts
  -> Serving Table: alerts
  -> API Endpoint: /alerts
  -> Validation Rule: raw_event_required_fields
  -> SLO: DLQ Rate
  -> Test Reference: tests/test_tenant_access_and_cache.py::test_tenant_principal_blocks_cross_tenant_access
```

Acceptance criteria:
- Alerts must be tenant scoped and queryable without cross-tenant leakage.

### OPS-003: Are data products fresh?

```text
OPS-003
  -> Consumer: Operations
  -> Business Question: Are data products fresh?
  -> Data Product: Platform Health (platform_health)
  -> Metric: tenant_health_score
  -> Source Event: system.health
  -> Processing: cloudscale_serving_metrics_staleness_seconds gauge, refreshed from tenant_metrics_daily.updated_at and stream_window_metrics.updated_at by ops-console
  -> Serving Table: tenant_metrics_daily
  -> API Endpoint: /metrics/tenant_health_score
  -> Validation Rule: raw_event_freshness
  -> SLO: Event-Processing Freshness
  -> Test Reference: tests/test_observability.py::test_grafana_dashboard_panels_reference_real_metrics
```

Acceptance criteria:
- A stale data product must surface a health/freshness signal rather than appearing silently healthy.

