# Sample Artifacts

These files are tracked, static examples for local review and API shape inspection. They are not claimed as measured production output.

| Path | Purpose |
| --- | --- |
| `events/sample_events_v2.jsonl` | Valid event envelopes across order, payment, user, product, and system domains. |
| `benchmarks/local_ingestion_sample.json` | Representative local load-test result shape. |
| `quality/tenant_demo_quality_sample.json` | Example data quality score payload. |
| `dashboard/tenant_demo_dashboard_sample.json` | Example tenant dashboard response assembled from analytics APIs. |

Validate samples with:

```bash
PYTHONPATH=services/shared python scripts/validate_sample_artifacts.py
```

Generate live local versions with Docker Compose running:

```bash
python scripts/load_test_events.py --output benchmarks/results/local-run.json
PYTHONPATH=services/shared python scripts/run_data_quality_checks.py --pretty
PYTHONPATH=services/shared:services/processing-service python scripts/demo_mode.py
```
