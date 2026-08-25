# Sample Artifacts

The `samples/` directory gives reviewers concrete payloads without requiring Docker, Kafka, or Postgres to be running.

## What Is Included

| Artifact | What it demonstrates |
| --- | --- |
| `samples/events/sample_events_v2.jsonl` | Event envelope shape, tenant metadata, payload contracts, trace IDs, and domain event variety. |
| `samples/benchmarks/local_ingestion_sample.json` | Benchmark result schema used by `scripts/benchmark_report.py`. |
| `samples/quality/tenant_demo_quality_sample.json` | Data quality scoring output and check result shape. |
| `samples/dashboard/tenant_demo_dashboard_sample.json` | Analytics-service response composition for a tenant dashboard. |

## Validation

```bash
PYTHONPATH=services/shared python scripts/validate_sample_artifacts.py
```

The validator parses every JSON artifact and validates sample events through the same Pydantic `EventEnvelope` contracts used by the services.

## Production Honesty

These are static examples for local review and documentation. Measured evidence should come from:

- `scripts/load_test_events.py` for ingestion API benchmarks.
- `scripts/run_data_quality_checks.py` for quality scores.
- `scripts/demo_mode.py` for a local end-to-end demo run.
