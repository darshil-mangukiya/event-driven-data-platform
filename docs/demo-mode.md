# One-Command Demo Mode

Run:

```bash
make demo
```

The demo command:

1. Starts Docker Compose when Docker is available.
2. Waits for services.
3. Generates tenant-patterned local events.
4. Runs the local end-to-end API flow.
5. Runs data quality checks.
6. Renders benchmark evidence.
7. Writes `evidence/validation/demo-summary.json`.

Host-published local services bind to loopback by default. Demo dashboard and
schema registry authentication are not configured.

Dry-run the plan without starting anything:

```bash
python scripts/demo_mode.py --dry-run
```

Useful URLs after startup:

- Demo dashboard: http://localhost:8005/?tenant_id=tenant_demo
- Analytics API docs: http://localhost:8003/docs
- Metadata API docs: http://localhost:8004/docs
- Prometheus: http://localhost:9090
- MinIO: http://localhost:9001

## Data before you even run `make demo`

`database/init/003_local_demo_transactional_seed.sql` runs automatically on
the very first `docker compose up` against an empty PostgreSQL volume — before
step 3 above creates any additional event volume. It initializes
orders, payments, sessions, and derived daily metrics for all 3 seed tenants,
so:

- The demo dashboard and analytics endpoints (`/metrics/product_performance`,
  `/metrics/marketing_roi`, `/metrics/event_throughput`) show real numbers
  immediately, not an empty state.
- `scripts/reconcile_metrics.py`'s revenue/payment/customer-activity checks
  pass cleanly out of the box, instead of always showing drift on a fresh
  volume, addressing the empty-source reconciliation condition.
- Running `make demo` afterward layers additional generated events on top —
  it is additive, not a prerequisite for having *any* data to look at.

See [local workload generation](local-data-generation.md#fresh-database-initialization)
for what is seeded and the live-verification trace, including two runtime defects
verification identified and corrected.
