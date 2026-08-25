# Data Products

This document is the narrative companion to the machine-readable data
product registry at `contracts/data_products/registry.yml`, validated by
`scripts/validate_data_products.py` and generated into
[evidence/data-products/data-product-catalog.md](../evidence/data-products/data-product-catalog.md).

It extends — does not duplicate — two existing contracts:

- `metrics/contracts/tenant_daily_metrics.json` — the technical
  metric-formula contract (grain, formula, required fields). This registry
  references those metrics by name; it never redefines a formula.
- `docs/data-product-contract-template.md` — the original template this
  registry's fields are modeled on, now populated with 6 real products
  instead of left blank.

For who consumes these products and why, see
[docs/consumer-requirements.md](consumer-requirements.md). For the
end-to-end requirement traces, see
[evidence/data-products/requirements-traceability.md](../evidence/data-products/requirements-traceability.md).

## The Six Data Products

| Product | Domain | Modeled Consumers | API |
|---|---|---|---|
| Revenue & Order Metrics | finance | Finance | `/metrics/revenue` |
| Payment Health | finance | Finance, Risk | `/metrics/payment_success` |
| Customer Activity | product | Product | `/metrics/customers`, `/metrics/churn`, `/metrics/retention` |
| Product Performance | product | Product | `/metrics/product_performance` |
| Marketing ROI Proxy | marketing | Marketing | `/metrics/marketing_roi` |
| Platform Health | operations | Operations, Risk | `/metrics/tenant_health_score`, `/metrics/event_throughput`, `/alerts` |

These six products group the 9 metric-formula contracts in
`metrics/contracts/tenant_daily_metrics.json` (revenue, customers, churn,
retention, marketing_roi, product_performance, payment_success,
event_throughput, tenant_health_score) under consumer-facing groupings —
e.g. "Customer Activity" bundles customers + churn + retention because a
Product consumer would ask about all three together, not as three
unrelated APIs.

## Revenue & Order Metrics (`revenue`)

**Business questions**: What is revenue for a tenant and period? How fresh
is the result? Does serving revenue reconcile to processed orders?

**Lineage** (two parallel paths — see docs/streaming_architecture.md for
why both exist):

```text
order.created / order.updated
    -> Kafka platform.events.orders
    -> processing-service (async)
    -> processed_orders
    -> tenant_metrics_hourly -> tenant_metrics_daily
    -> /metrics/revenue
    -> Finance consumer

(parallel, not yet API-served)
order.created / order.updated
    -> Kafka platform.events.orders
    -> Spark Structured Streaming (validate -> dedup -> watermark -> window)
    -> stream_window_metrics
```

**Tenant isolation**: `assert_tenant_scope()` against the JWT/header-derived
`TenantPrincipal`, plus a SQL `WHERE tenant_id = $1` filter in
`AnalyticsRepository.revenue()`. Covered by
`tests/test_tenant_access_and_cache.py::test_tenant_principal_blocks_cross_tenant_access`.

**Cache**: Redis, 120s TTL, key scoped by `tenant_id` + query params
(`stable_cache_key`). On Redis outage, falls back to PostgreSQL — the
request still succeeds, just uncached (see the redis-outage
reliability exercise, which identified and corrected the original crash-on-outage
bug).

**Reconciliation**: `scripts/reconcile_metrics.py::evaluate_reconciliation`
compares `tenant_metrics_daily` against a fresh aggregate over
`processed_orders`, tolerance `0.01`.

**Known limitation — the streaming/serving gap**: the Structured Streaming
pipeline computes its own windowed revenue into
`stream_window_metrics`, but the analytics API's `/metrics/revenue`
endpoint still serves from the async processing-service's
`tenant_metrics_daily`. The two paths are architecturally parallel (see
docs/streaming_architecture.md), not yet merged into one served value.
This is stated plainly rather than implied to be wired together.

## Payment Health (`payment_health`)

**Business questions**: What is the payment success/failure rate for a
tenant? Are payment failures indicating abnormal behavior?

**Lineage**:

```text
payment.authorized / payment.captured / payment.failed
    -> Kafka platform.events.payments
    -> processing-service
    -> processed_payments
    -> tenant_metrics_daily
    -> /metrics/payment_success
    -> Finance / Risk consumer
```

A high-risk failed payment additionally produces an alert signal (see
`tests/test_processing_logic.py::test_high_risk_failed_payment_creates_alert_signal`),
which makes this product relevant to both the Risk and Finance consumers.

**Reconciliation** : `scripts/reconcile_metrics.py::evaluate_payment_reconciliation`
compares `tenant_metrics_daily.payment_success_count`/`payment_failure_count`
against a fresh recomputation from `processed_payments`, `count_tolerance=0`.
Verified live against a real database — see `docs/reconciliation.md`
"Runtime Verification". This closes what was previously an open,
documented gap ("no dedicated payment reconciliation script exists yet").

## Customer Activity (`customer_activity`)

**Business questions**: Which products are performing well? How is
customer/product activity changing?

Bundles `customers`, `churn`, and `retention` — see
[docs/metric-definitions.md](metric-definitions.md) "Proxy Metrics" for
exactly what the churn signal and retention proxy do and do not represent.
Neither is a confirmed-churn or cohort-retention calculation.

**Reconciliation** : `scripts/reconcile_metrics.py::evaluate_customer_activity_reconciliation`
compares `tenant_metrics_daily.new_users`/`active_users`/`churn_signal_count`
against a fresh recomputation from `processed_user_sessions`,
`count_tolerance=0`. Verified live — see `docs/reconciliation.md`.

## Product Performance (`product_performance`)

**Business questions**: Which products generate the most activity or
revenue?

Unlike the other five products, this one is **query-time computed** (a
join of `processed_orders` against `tenant_products` metadata), not served
from a pre-aggregated rollup table — documented in the registry's
`processing` field rather than implied to work like the others.

## Marketing ROI Proxy (`marketing_performance`)

**Business question**: What marketing-performance proxy is available from
the modeled event data?

This is explicitly a **proxy**: local MVP spend attribution, not a
production multi-touch attribution model or a real ad-platform spend feed
integration. `scripts/validate_data_products.py`'s proxy-labeling check
(`validate_proxy_labeling`) fails the build if this product's description
ever drops the word "proxy" or starts using stronger attribution-model
language.

## Platform Health (`platform_health`)

**Business questions**: Is event processing healthy? Are data products
fresh? Is processing lag increasing? Are failures accumulating in the DLQ?
Which alerts require review?

This is the product most directly connected to the affected components:

```text
system.health / system.alert -> Kafka platform.events.system
    -> service_health_metrics / alerts
    -> /metrics/tenant_health_score, /alerts
    -> Operations / Risk consumer

(streaming observability path)
Kafka domain topics -> Spark Structured Streaming
    -> cloudscale_stream_* Prometheus metrics
    -> Grafana dashboard / alert rules (docs/OBSERVABILITY.md)
```

Unlike the other five products, this one is **not cached** —
`/metrics/tenant_health_score` is a query-time composite computed fresh on
every request (`services/analytics-service/app/main.py`), so there is no
cache/fallback contract to document for it.

**Known limitation**: `tenant_health_score`'s weights are MVP defaults, not
calibrated against real incidents — stated in both the registry's
`known_limitations` and its acceptance criteria.

## Cross-Reference Validation

Every field above that names an API endpoint, source event, serving table,
metric, or SLO is validated against the real system, not hand-verified:

```bash
PYTHONPATH=.:services/shared python scripts/validate_data_products.py
```

This imports the real analytics-service FastAPI app and checks
`api_endpoint` against its live OpenAPI route table, parses
`contracts/registry.json` for `source_events`, parses
`catalog/data_catalog.json` for `serving_table`/`source_tables`, parses
`metrics/contracts/tenant_daily_metrics.json` for `metric_contracts`, and
parses `docs/slo-catalog.md`'s SLO table for `slo_reference`. See
`data_products/registry.py` for how each check derives its "real system
fact" rather than comparing against a second hand-maintained list.

## CLI

```bash
python -m platform_cli data-products list
python -m platform_cli data-products show revenue
python -m platform_cli data-products validate
python -m platform_cli data-products trace FIN-001
python -m platform_cli data-products generate
```

## Make targets

```bash
make data-products-list
make data-products-validate
make data-products-catalog
make requirements-trace
make data-products-test
```
