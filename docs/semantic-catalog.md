# Semantic Catalog

This catalog maps business metrics to dimensions, measures, API endpoints, and BI-ready usage.

## Dimensions

| Dimension | Source | Used By |
| --- | --- | --- |
| `tenant_id` | all core tables | all metrics |
| `metric_date` | `tenant_metrics_daily` | revenue, customers, churn, retention, marketing ROI, payment success |
| `product_id` | `processed_orders`, `tenant_products` | product performance |
| `category` | `tenant_products` | product performance |
| `marketing_campaign_id` | `processed_orders`, `processed_user_sessions` | marketing ROI, retention analysis |
| `region` | `processed_orders`, `tenant_config` | revenue segmentation |

## Measures

| Measure | Formula | Endpoint |
| --- | --- | --- |
| `net_revenue` | sum net order revenue | `/metrics/revenue` |
| `average_order_value` | net revenue / order count | `/metrics/revenue` |
| `active_users` | distinct active users | `/metrics/customers` |
| `churn_signal_count` | count churn signal events | `/metrics/churn` |
| `retention_rate` | 1 - churn signals / active users | `/metrics/retention` |
| `marketing_roi` | attributed revenue / spend | `/metrics/marketing_roi` |
| `payment_success_rate` | successes / payment attempts | `/metrics/payment_success` |
| `tenant_health_score` | weighted operational composite | `/metrics/tenant_health_score` |

## Dashboard Mapping

- Executive overview: revenue, customers, tenant health.
- Finance dashboard: revenue, AOV, payment success.
- Marketing dashboard: ROI and campaign-attributed revenue.
- Product dashboard: product performance and active users.
- Operations dashboard: system status, throughput, alerts, cache health.
