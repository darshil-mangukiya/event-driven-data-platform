# PostgreSQL Query-Plan Verification

Scope: Local verification
Date: 2026-08-22

## Existing Indexes

Before adding any index, `database/init/001_schema.sql` was inspected for
existing indexing. Finding: **the schema is already broadly
indexed** for its actual query patterns — 19 composite indexes already
exist (`idx_processed_orders_tenant_date`, `idx_processed_orders_product`,
`idx_processed_orders_campaign`, `idx_processed_payments_status`,
`idx_raw_events_tenant_timestamp`, a GIN index on `raw_events.payload`,
and more), and `tenant_metrics_daily` — the table every analytics-API
endpoint reads (`repository.py::revenue`, etc.) — has a composite primary
key `(tenant_id, metric_date)` that **already exactly matches** the
`WHERE tenant_id = $1 AND metric_date BETWEEN $2 AND $3` shape every
serving query uses.

`EXPLAIN (ANALYZE, BUFFERS)` confirms that the relevant local queries select
the existing indexes.

## Live Verification

```
$ POSTGRES_HOST_PORT=15432 docker compose up -d postgres
$ docker exec -i ...-postgres-1 psql -U platform -d data_platform
```

**Revenue query** (`services/analytics-service/app/repository.py::revenue`,
the exact SQL shape, against the real seeded `tenant_metrics_daily`):
```sql
explain (analyze, buffers)
select metric_date, gross_revenue, net_revenue, order_count, units_sold
from tenant_metrics_daily
where tenant_id = 'tenant_demo' and metric_date between '2026-01-01' and '2026-12-31'
order by metric_date desc limit 30 offset 0;
```
→ `Index Scan Backward using tenant_metrics_daily_pkey` — **not** a
sequential scan. `Execution Time: 0.040 ms`.

**Product performance query** (`processed_orders`, tenant + product
filter):
```sql
explain (analyze, buffers)
select product_id, count(*), sum(gross_revenue)
from processed_orders
where tenant_id = 'tenant_demo' and product_id = 'prod_001'
group by product_id;
```
→ `Index Scan using idx_processed_orders_product` — **not** a sequential
scan. `Execution Time: 0.083 ms`.

Full results: [benchmarks/postgres_query_benchmarks.csv](../../benchmarks/postgres_query_benchmarks.csv).

## Scope

- No before/after missing-index comparison was run because the relevant
  indexes are part of the current schema.
- **No large benchmark dataset was created.** At this
  project's seed-data scale (880 orders, 6 daily-metric rows — see
  `docs/local-data-generation.md`), every query executes in under a millisecond
  regardless of index usage; the *plan* (Index Scan vs. Seq Scan)
  confirms plan selection, but a measurable before/after timing delta would
  require a controlled, much larger dataset. Generating one was out of
  scope for verification — `scripts/load_test_events.py`/
  `scripts/generate_synthetic_events_v2.py` already exist and could
  produce one for a dedicated capacity study.
- **No table/date partitioning was introduced.** Per the explicit
  instruction ("Only introduce native partitioning if query shape
  benefits, table design supports it, migration complexity is justified
  ... do not partition small tables just for a keyword") — at this
  project's scale, native partitioning would add real operational
  complexity (partition maintenance, migration risk) with zero measurable
  benefit; not introduced.
- **Connection pooling:** `platform_shared.database.Postgres` wraps
  `asyncpg.create_pool` with `min_size=1` and `max_size=10`.
- **No production-scale claim.** All timings above are from this local
  environment's seed-scale data; see `docs/LIMITATIONS.md`.
