# Consumer Requirements

> **These are modeled internal-consumer requirements for the local platform.**

This document is the narrative companion to the machine-readable contracts
in `contracts/data_products/consumers.yml` and
`contracts/data_products/requirements.yml`. See
[docs/data-products.md](data-products.md) for the data-product-by-product
detail and
[evidence/data-products/requirements-traceability.md](../evidence/data-products/requirements-traceability.md)
for the generated, fully cross-referenced trace of every requirement.

## Why this exists

Structured Streaming, reliability exercises, and observability provide the
technical platform. These contracts connect that platform to modeled consumers
and their questions, making ownership and requirements explicit and traceable.

## Modeled Consumers

### Finance

**Modeled responsibility**: Tracks tenant revenue, order volume, and
payment health to support modeled revenue reporting and reconciliation
workflows.

| | |
|---|---|
| Business questions | What is revenue for a tenant and period? How fresh is the result? Does serving revenue reconcile to processed orders? What is the payment success/failure rate for a tenant? |
| Required data products | [Revenue & Order Metrics](data-products.md#revenue--order-metrics-revenue), [Payment Health](data-products.md#payment-health-payment_health) |
| Freshness expectation | 15 minutes (`/metrics/revenue`, per `docs/metric-sla.md`) |
| Latency expectation | p95 < 300ms cached / < 750ms uncached |
| Tenant boundary | Authenticated tenant may retrieve only its own records — enforced by `assert_tenant_scope()` |
| Failure expectations | Redis outage → PostgreSQL fallback (request still succeeds); PostgreSQL outage → controlled 5xx, not a silent zero |
| Relevant API endpoints | `/metrics/revenue`, `/metrics/payment_success` |
| Relevant SLOs | Serving Freshness (Async Path), Reconciliation Correctness |

### Product

**Modeled responsibility**: Tracks customer activity, product performance,
and engagement signals to support modeled product-analytics workflows.

| | |
|---|---|
| Business questions | Which products are performing well? How is customer/product activity changing? Which products generate the most activity or revenue? |
| Required data products | [Customer Activity](data-products.md#customer-activity-customer_activity), [Product Performance](data-products.md#product-performance-product_performance) |
| Freshness expectation | 30–60 minutes |
| Latency expectation | p95 < 750ms uncached |
| Tenant boundary | Same `assert_tenant_scope()` mechanism as Finance |
| Failure expectations | Same Redis/PostgreSQL fallback behavior as Finance |
| Relevant API endpoints | `/metrics/customers`, `/metrics/churn`, `/metrics/retention`, `/metrics/product_performance` |
| Relevant SLOs | Serving Freshness (Async Path) |

### Marketing

**Modeled responsibility**: Tracks the marketing-performance proxy available
from modeled event data — a local approximation, not a production
attribution model.

| | |
|---|---|
| Business questions | What marketing-performance proxy is available from the modeled event data? |
| Required data products | [Marketing ROI Proxy](data-products.md#marketing-roi-proxy-marketing_performance) |
| Freshness expectation | 60 minutes |
| Latency expectation | 99.0% monthly availability |
| Tenant boundary | Same `assert_tenant_scope()` mechanism as Finance |
| Failure expectations | Same Redis/PostgreSQL fallback behavior as Finance |
| Relevant API endpoints | `/metrics/marketing_roi` |
| Relevant SLOs | Serving Freshness (Async Path) |

This product is a **marketing ROI proxy** computed from simplified local
spend attribution. It is not a multi-touch attribution model.

### Operations

**Modeled responsibility**: Tracks event processing health, data-product
freshness, processing lag, and DLQ volume to support modeled
platform-operations workflows.

| | |
|---|---|
| Business questions | Is event processing healthy? Are data products fresh? Is processing lag increasing? Are failures accumulating in the DLQ? |
| Required data products | [Platform Health](data-products.md#platform-health-platform_health) |
| Freshness expectation | 5 minutes (`/system/status` analog) |
| Latency expectation | 99.9% monthly availability |
| Tenant boundary | Same `assert_tenant_scope()` mechanism as Finance |
| Failure expectations | Streaming checkpoint stale → `StreamingCheckpointStale` alert fires (not silently quiet); reliability exercise failure → `cloudscale_reliability_exercise_last_status` drops to 0 |
| Relevant API endpoints | `/metrics/tenant_health_score`, `/metrics/event_throughput`, `/alerts` |
| Relevant SLOs | Streaming Availability, Processing Lag, DLQ Rate, Checkpoint Freshness |

This modeled consumer is the primary audience for the Grafana dashboard and
the alert rules maintained with the platform.

### Risk

**Modeled responsibility**: Tracks payment failures and platform health
signals for abnormal-behavior review, mapped only to capabilities the
repository implements.

| | |
|---|---|
| Business questions | Are payment failures or platform signals indicating abnormal behavior? Which alerts require review? |
| Required data products | [Payment Health](data-products.md#payment-health-payment_health), [Platform Health](data-products.md#platform-health-platform_health) |
| Freshness expectation | Same as Finance/Operations for the underlying products |
| Latency expectation | Same as Finance/Operations for the underlying products |
| Tenant boundary | Same `assert_tenant_scope()` mechanism as Finance |
| Failure expectations | High-risk failed payments produce an alert signal, not a silent record (see `tests/test_processing_logic.py::test_high_risk_failed_payment_creates_alert_signal`) |
| Relevant API endpoints | `/metrics/payment_success`, `/alerts` |
| Relevant SLOs | Serving Freshness (Async Path), DLQ Rate |

Risk is not mapped to a dedicated fraud-detection or credit-risk product
because the platform does not implement one. No risk score is defined.

## Requirements Traceability

Every business question above is backed by a machine-readable requirement
in `contracts/data_products/requirements.yml`, validated by
`scripts/validate_data_products.py` and traced end-to-end by
`data_products/validator.py::validate_traceability`. See
[evidence/data-products/requirements-traceability.md](../evidence/data-products/requirements-traceability.md)
for the full, generated trace of every requirement — consumer → business
question → data product → metric → source event → processing → serving
table → API → validation rule → SLO → acceptance criteria.

Query one requirement directly:

```bash
PYTHONPATH=.:services/shared python -m platform_cli --pretty data-products trace FIN-001
```

## Modeled Scope

- Business questions connect to implemented API endpoints, serving tables,
  validation rules, and SLO definitions.
- Every "modeled owner" (e.g. "Finance Analytics" for revenue) is an
  architecture and domain-ownership label.
