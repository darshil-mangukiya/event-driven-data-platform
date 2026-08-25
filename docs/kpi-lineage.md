# KPI Lineage

For the data-product-contract view of this same lineage (consumer,
freshness, SLO, tenant isolation, acceptance criteria), see
[docs/data-products.md](data-products.md) and the machine-readable
`contracts/data_products/registry.yml`. For the full, cross-reference-validated
table-level lineage graph covering all tables in addition to KPIs, generated from
`catalog/data_catalog.json`, see [docs/lineage.md](lineage.md) and
[evidence/lineage/lineage-graph.md](../evidence/lineage/lineage-graph.md).

## Net Revenue

```mermaid
flowchart LR
    Producer["Checkout / Order Producer"] --> Ingestion["Ingestion Service"]
    Ingestion --> Kafka["platform.events.orders"]
    Kafka --> Processing["Processing Service"]
    Processing --> Orders["processed_orders"]
    Orders --> Daily["tenant_metrics_daily"]
    Daily --> API["/metrics/revenue"]
    Orders --> DBT["dbt fct_tenant_daily_metrics"]
    Daily --> Reconcile["scripts/reconcile_metrics.py"]
    Kafka --> Streaming["Spark Structured Streaming\n(validate/dedup/watermark/window)"]
    Streaming --> StreamMetrics["stream_window_metrics"]
    StreamMetrics -. not yet API-served .-> API
```

The Structured Streaming path  is a second, parallel lineage for
the same source events — it computes its own windowed revenue into
`stream_window_metrics` with event-time semantics the async path doesn't
have (watermarking, late-event handling, exactly-the-window-you-asked-for
aggregation). It is **not yet merged** into the value `/metrics/revenue`
serves; the dotted line above marks that gap explicitly rather than
implying the two paths are already unified. See
`docs/streaming_architecture.md` for why both paths exist side by side.

## Payment Success Rate

```mermaid
flowchart LR
    Producer["Payments Producer"] --> Ingestion["Ingestion Service"]
    Ingestion --> Kafka["platform.events.payments"]
    Kafka --> Processing["Processing Service"]
    Processing --> Payments["processed_payments"]
    Payments --> Daily["tenant_metrics_daily"]
    Daily --> PaySuccess["/metrics/payment_success"]
    Daily --> Health["/metrics/tenant_health_score"]
    Processing --> Alerts["alerts (high-risk failed payment)"]
    Alerts --> RiskConsumer["Risk consumer"]
    PaySuccess --> FinanceConsumer["Finance consumer"]
```

## Product Performance

```mermaid
flowchart LR
    Orders["processed_orders"] --> ProductPerf["/metrics/product_performance"]
    Products["tenant_products"] --> ProductPerf
    ProductPerf --> BI["BI dashboard / internal consumers"]
```

## Tenant Health Score Inputs

Tenant health score is an operational composite. It should not be treated as a finance metric.

Inputs:

- payment failure rate
- event throughput
- processed event volume
- service health summaries
- cache hit/miss trend
- Kafka lag indicators

## Customer Growth

```mermaid
flowchart LR
    UserEvents["platform.events.users"] --> Processing["Processing Service"]
    Processing --> Sessions["processed_user_sessions"]
    Sessions --> Daily["tenant_metrics_daily"]
    Daily --> API["/metrics/customers"]
```

## Churn and Retention

```mermaid
flowchart LR
    UserEvents["user.churn_signal / user.activity"] --> Sessions["processed_user_sessions"]
    Sessions --> Daily["tenant_metrics_daily"]
    Daily --> Churn["/metrics/churn"]
    Daily --> Retention["/metrics/retention"]
```

## Marketing ROI

```mermaid
flowchart LR
    Orders["processed_orders with marketing_campaign_id"] --> Daily["tenant_metrics_daily"]
    Daily --> ROI["/metrics/marketing_roi"]
    ROI --> Marketing["Marketing analytics consumers"]
```

## Event Throughput

```mermaid
flowchart LR
    Kafka["Kafka domain topics"] --> Processing["Processing Service"]
    Processing --> Aggregates["tenant_metrics_daily.events_processed"]
    Processing --> Watermarks["pipeline_watermarks"]
    Aggregates --> API["/metrics/event_throughput"]
    Watermarks --> Ops["Ops Console / freshness checks"]
```
