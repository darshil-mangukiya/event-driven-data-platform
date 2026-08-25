# Metric Definitions

This document gives the full business + technical definition for every
metric in `metrics/contracts/tenant_daily_metrics.json`. It extends
`docs/metric-ownership.md` (ownership + reconciliation summary table) and
`docs/metric-sla.md` (freshness/availability targets) with the detail those
tables don't carry: business meaning, technical formula, grain, dimensions,
and — critically — explicit proxy/limitation labeling.

Only metrics that exist in the metric contract are documented
here; this is not an aspirational metric catalog.

## Revenue

- **Business meaning**: Tenant gross and net revenue for a period, plus
  order volume and average order value.
- **Technical definition**: `net_revenue = sum(order.quantity * order.unit_price - discount_amount)`;
  `average_order_value = net_revenue / nullif(order_count, 0)`.
- **Grain**: `(tenant_id, metric_date)`.
- **Dimensions**: `tenant_id, metric_date, region, channel`.
- **Source**: `processed_orders` (async processing-service path).
- **Serving table**: `tenant_metrics_daily`.
- **API**: `/metrics/revenue`.
- **Freshness**: 15 minutes.
- **Validation**: `processed_order_revenue_ranges` (no negative revenue/quantity/price).
- **SLO**: Serving Freshness (Async Path), Reconciliation Correctness.
- **Known limitations**: Local initialization values are for development and should
  not be interpreted as financial evidence.

## Order Count / Units Sold

- **Business meaning**: Order volume and units moved, alongside revenue.
- **Technical definition**: Counted directly from `processed_orders` rows
  in the rollup window; not a separate contract, part of the `revenue`
  metric's `required_fields`.
- **Grain / Source / Serving / API**: Same as Revenue.

## Average Order Value

- **Business meaning**: Net revenue divided by order count.
- **Technical definition**: `net_revenue / nullif(order_count, 0)`.
- **Grain / Source / Serving / API**: Same as Revenue.

## Payment Failure Rate (Payment Success)

- **Business meaning**: How often tenant payments fail vs. succeed.
- **Technical definition**: `success_rate = payment_success_count / nullif(payment_success_count + payment_failure_count, 0)`.
- **Grain**: `(tenant_id, metric_date)`.
- **Dimensions**: `tenant_id, metric_date, payment_method`.
- **Source**: `processed_payments`.
- **Serving table**: `tenant_metrics_daily`.
- **API**: `/metrics/payment_success`.
- **Freshness**: 15 minutes (near-real-time, same cadence as revenue).
- **Validation**: `raw_event_required_fields`.
- **SLO**: Serving Freshness (Async Path).
- **Known limitations**: Local data does not model all processor states
  such as chargebacks or refunds.

## Customer Activity

- **Business meaning**: New and active customer counts.
- **Technical definition**: `new_users = count(distinct user_id where action = 'signed_up')`;
  `active_users = count(distinct user_id)`.
- **Grain**: `(tenant_id, metric_date)`.
- **Dimensions**: `tenant_id, metric_date, plan, marketing_campaign_id`.
- **Source**: `processed_user_sessions`.
- **Serving table**: `tenant_metrics_daily`.
- **API**: `/metrics/customers`.
- **Freshness**: 30 minutes.
- **Known limitations**: Anonymous events require identity stitching before
  production use.

## Product Performance

- **Business meaning**: Product-level revenue and units sold.
- **Technical definition**: `net_revenue = sum(processed_orders.net_revenue)`;
  `units_sold = sum(quantity)`, joined against `tenant_products` metadata.
- **Grain**: `(tenant_id, product_id)`.
- **Dimensions**: `tenant_id, product_id, category`.
- **Source**: `processed_orders`, `tenant_products` (query-time join, not a
  pre-aggregated table).
- **API**: `/metrics/product_performance`.
- **Freshness**: 60 minutes.
- **Known limitations**: Inventory state is event-derived and should be
  reconciled with source-of-truth catalog systems.

## Event Throughput

- **Business meaning**: Processed event volume per tenant — the
  "is the pipeline keeping up" signal.
- **Technical definition**: `events_processed = order_events + payment_events + user_events`.
- **Grain**: `(tenant_id, metric_date)`.
- **Dimensions**: `tenant_id, metric_date, source_topic`.
- **Source**: `tenant_metrics_daily`, `pipeline_watermarks`,
  `service_health_metrics` (async path); `cloudscale_stream_events_processed_total`
  Prometheus counter (streaming path, the affected components — not yet merged into
  this API response).
- **API**: `/metrics/event_throughput`.
- **Freshness**: 15 minutes.
- **Known limitations**: Kafka lag and exact offset accounting require live
  broker telemetry not available in this local environment.

## Tenant Health Score

- **Business meaning**: Composite operational health score combining
  payment failures, churn signals, processed event volume, and service
  health indicators.
- **Technical definition**: `100 - weighted penalties for payment
  failures, churn signals, low event volume, and stale platform health`.
- **Grain**: `(tenant_id)`.
- **Source**: `tenant_metrics_daily`, `service_health_metrics`, `alerts`.
- **API**: `/metrics/tenant_health_score`.
- **Freshness**: 5 minutes (query-time composite, not cached).
- **Known limitations**: Score weights are MVP defaults and should be
  calibrated against real incidents. This is an **operational** composite,
  not a finance metric — see `docs/kpi-lineage.md`'s "Tenant Health Score
  Inputs" section.

## Processing Lag

- **Business meaning**: How far behind real-time the Structured Streaming
  pipeline is running.
- **Technical definition**: `cloudscale_stream_processing_lag_seconds`,
  read from Spark's own `query.lastProgress.batchDuration` — a direct
  measurement, not an estimate.
- **Grain**: per streaming query (`aggregates`, `dlq`, `dedup-audit`,
  `received-metrics`).
- **Source**: Spark Structured Streaming driver process .
- **Serving surface**: Prometheus `/metrics` on the `spark-streaming`
  service .
- **Freshness**: real-time (updated on every micro-batch).
- **SLO**: Processing Lag (`docs/slo-catalog.md`).
- **Known limitations**: This is a platform-operations signal, not exposed
  through the analytics-service API — it's a Prometheus/Grafana metric,
  consumed by the modeled Operations consumer via the dashboard, not via
  `/metrics/*`.

---

## Proxy Metrics

These four metrics are heuristics or signals. Their labels are enforced by
`data_products.validator.validate_proxy_labeling`.

### Churn Signal

- **What it represents**: A count of `churn_signal`/`cancel_intent` user
  events — a leading indicator someone is expressing churn intent.
- **What inputs it uses**: `processed_user_sessions` rows where
  `action in ('churn_signal', 'cancel_intent')`.
- **What it does NOT represent**: Confirmed contractual churn (an actual
  cancellation or non-renewal). It is a *signal*, not a churn-rate
  calculation against a subscription lifecycle.

### Retention Proxy

- **What it represents**: `retention_rate = 1 - churn_signal_count / nullif(active_users, 0)`
  — an approximation of retention derived from the churn signal above.
- **What inputs it uses**: `active_users` and `churn_signal_count` from
  `tenant_metrics_daily`.
- **What it does NOT represent**: True cohort-based retention (e.g.
  "what fraction of users active in month N are still active in month
  N+1"). That requires a longer-lived identity and subscription model this
  platform doesn't implement.

### Marketing ROI Proxy

- **What it represents**: `marketing_roi = marketing_attributed_revenue / nullif(marketing_spend, 0)`,
  using `processed_orders` rows tagged with a `marketing_campaign_id`.
- **What inputs it uses**: Simplified local spend attribution — the
  campaign ID travels with the order event; there's no multi-touch
  attribution model.
- **What it does NOT represent**: Multi-touch marketing attribution.
  There is no ad-platform spend feed integration; `marketing_spend` is
  local development values.

### Tenant Health Score

- **What it represents**: A composite 0–100 operational score, weighted
  from payment failures, churn signals, event volume, and platform health
  staleness.
- **What inputs it uses**: `tenant_metrics_daily`, `service_health_metrics`,
  `alerts`.
- **What it does NOT represent**: A calibrated, incident-validated health
  score. The weights are MVP defaults (see `docs/kpi-lineage.md`).

## Metric Ownership (Modeled)

These are architecture/domain ownership labels for design clarity — not
claims about an actual company's org chart. See
`docs/metric-ownership.md` for the canonical ownership table; this section
only restates it in the same terms this document uses.

| Metric | Modeled Owner |
|---|---|
| Revenue | Finance Analytics |
| Payment Health | Finance / Risk |
| Customer Activity | Product Analytics |
| Product Performance | Product Analytics |
| Marketing ROI Proxy | Marketing Analytics |
| Event Throughput | Data Platform |
| Tenant Health Score | Data Platform |
