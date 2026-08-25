# Metric Ownership

Metrics are treated as platform contracts. Each metric has an owner, source lineage, serving endpoint, and reconciliation expectation.

See also [docs/metric-definitions.md](metric-definitions.md) for full
business/technical definitions, formulas, and proxy-metric caveats, and
[docs/data-products.md](data-products.md) for how these metrics group into
consumer-facing data products with modeled requirements.

| Metric | Owner | Source tables | Serving surface | Quality/reconciliation |
| --- | --- | --- | --- | --- |
| Net revenue | Finance analytics | `processed_orders` | `/metrics/revenue`, `fct_tenant_daily_metrics` | Revenue drift tolerance `$0.01`. |
| Customer growth | Product analytics | `processed_user_sessions` | `/metrics/customers` | New/active user count checks. |
| Churn signal count | Product analytics | `processed_user_sessions` | `/metrics/churn` | Action validity and volume checks. |
| Retention proxy | Product analytics | `tenant_metrics_daily` | `/metrics/retention` | Active-user freshness checks. |
| Marketing ROI | Marketing analytics | `processed_orders` | `/metrics/marketing_roi` | Campaign attribution completeness. |
| Product performance | Operations analytics | `processed_orders`, `tenant_products` | `/metrics/product_performance` | Product reference coverage. |
| Payment success rate | Finance/risk | `processed_payments` | `/metrics/tenant_health_score` | Payment status validity. |
| Tenant health score | Platform | metrics, health, payment facts | `/metrics/tenant_health_score` | Service health freshness and failure-rate thresholds. |

## Change Control

1. Propose metric definition change.
2. Identify affected APIs, dbt models, dashboards, and docs.
3. Add or update reconciliation/quality checks.
4. Backfill affected serving tables.
5. Announce semantic change to consumers.
