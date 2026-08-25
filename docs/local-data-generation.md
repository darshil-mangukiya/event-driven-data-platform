# Local Workload Generation

The deterministic local workload generator creates tenant-specific event
patterns instead of uniform random noise. Its configurable behavior includes:

- tenant order rates and average order values;
- regional, product-category, and campaign-attribution distributions;
- payment-failure and churn-signal rates; and
- time-of-day and weekday seasonality.

The implementation retains the historical command name
`scripts/generate_synthetic_events_v2.py`. Given the same seed and arguments,
it produces the same contract-valid event sequence, including stable event and
idempotency identifiers. Events can be written to JSONL or posted to ingestion:

```bash
PYTHONPATH=services/shared python scripts/generate_synthetic_events_v2.py \
  --count 5000 \
  --output data/synthetic/events_v2.jsonl

PYTHONPATH=services/shared python scripts/generate_synthetic_events_v2.py \
  --count 5000 \
  --post-to-ingestion
```

This generator runs only when invoked. Count and output arguments control local
volume without changing service configuration.

## Fresh-database initialization

`database/init/003_local_demo_transactional_seed.sql` runs after
`002_seed.sql` on a fresh PostgreSQL volume. It uses `generate_series`, CTEs,
and window functions to initialize:

- `processed_orders`, with tenant-specific volumes and price ranges;
- `raw_events`, using the corresponding order `event_id` for referential
  consistency;
- `processed_payments`, with tenant-specific failure rates;
- `alerts`, capped per tenant so each tenant is represented;
- `processed_user_sessions`, with signup, churn, and page-view activity; and
- `tenant_metrics_daily`, derived from the processed tables with the same
  aggregation shape as `scripts/backfill_metrics.py`.

Deriving the daily metrics from their source tables keeps reconciliation
meaningful and avoids maintaining two independent copies of the same values.

## Timestamp and distribution controls

- Payment timestamps are clamped to `23:59:59` on the order date so a
  two-minute offset cannot create an unintended next-day aggregate.
- Alert selection uses `row_number() over (partition by tenant_id ...)` and a
  per-tenant cap, preventing one tenant from consuming the entire alert set.
- Order, price, payment, and activity distributions are specified per tenant in
  both the Python generator and SQL initialization path.

## Verification

Fresh-volume verification confirms that all processed-layer tables contain all
three local tenants, the six expected daily aggregate rows are present, and the
revenue, payment, and customer-activity reconciliation checks pass. Structural
and optional live coverage is in `tests/test_local_demo_seed_data.py`.

The related reachability regression is covered separately in
`tests/reliability/test_reachability.py`: subsecond PostgreSQL timeouts are
rounded up before being passed to psycopg2, whose `connect_timeout` is an
integer number of seconds.
