# OpenLineage-Style Tracking

The project includes a lightweight OpenLineage-style event shape for local job tracking.

Emit a dry-run lineage event manually:

```bash
PYTHONPATH=services/shared python scripts/emit_lineage_event.py \
  --job-name backfill_tenant_metrics_daily \
  --tenant-id tenant_demo \
  --inputs processed_orders,processed_payments,processed_user_sessions \
  --outputs tenant_metrics_daily \
  --status succeeded \
  --dry-run
```

Persisted events go to `lineage_events`.

Sample artifact:

```text
openlineage/sample_lineage_event.json
```

## Real Emission

The manual CLI above is no longer the only way an event reaches
`lineage_events`. `lineage/events.py` gives real pipeline code a single
call — `emit_pipeline_lineage()` — that builds and persists an event
correlated by the *same run_id* that pipeline already uses for its own
`pipeline_run_log` row, reusing `build_lineage_event`/`insert_lineage_event`
from this module rather than a second implementation. Three pipelines call
it today:

- `scripts/backfill_metrics.py::run_backfill`
- `scripts/reconcile_metrics.py::run_reconciliation`
- `spark/streaming/sinks.py::PostgresSink` (a synchronous psycopg2
  equivalent, `_write_lineage_event`, since Spark's driver thread isn't
  running an asyncio event loop)

Verified live against a real Docker-composed PostgreSQL: the backfill
run's `pipeline_run_log.pipeline_run_id` and the reconciliation run's own
generated `run_id` were confirmed, by direct SQL query afterward, to
exactly match the `run_id` on their respective `lineage_events` rows — see
[docs/lineage.md](lineage.md) "Runtime Verification" for the full trace,
and "What this framework itself caught" for two runtime defects this live
verification found in the persistence path itself.

## Production Evolution

- ~~correlate `pipeline_run_log`, `lineage_events`, and reconciliation audit rows by run ID~~ — done for backfill and reconciliation ; see above.
- emit lineage events from dbt runs and data quality checks (not yet wired)
- publish to Marquez/OpenLineage or a managed metadata platform
