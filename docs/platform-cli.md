# Platform CLI

The `platform_cli` package provides local operator workflows for common platform tasks.

## Commands

| Command | Purpose |
| --- | --- |
| `python -m platform_cli tenant create` | Create a tenant, seed users/events/metrics, and issue a local service account token. |
| `python -m platform_cli tenant validate` | Validate tenant readiness checks. |
| `python -m platform_cli replay dlq` | Delegate DLQ replay to `scripts/dlq_tool.py`. |
| `python -m platform_cli backfill metrics` | Build or execute a tenant daily metrics backfill. |
| `python -m platform_cli health check` | Run release/preflight checks. |
| `python -m platform_cli evidence generate` | Generate the local validation evidence bundle. |
| `python -m platform_cli config validate` | Validate environment-aware settings. |
| `python -m platform_cli ops watermarks` | Inspect pipeline checkpoint/watermark state. |
| `python -m platform_cli ops reconciliation` | Summarize reconciliation audit status. |

## Dry-Run First

Most operational commands support `--dry-run`. Dry-runs are intentionally useful without Docker, Kafka, or PostgreSQL running.

## Examples

```bash
PYTHONPATH=.:services/shared python -m platform_cli --pretty config validate
PYTHONPATH=.:services/shared python -m platform_cli --pretty health check --dry-run
PYTHONPATH=.:services/shared python -m platform_cli --pretty evidence generate --output-dir evidence/validation
PYTHONPATH=.:services/shared python -m platform_cli --pretty backfill metrics --tenant-id tenant_demo --start-date 2026-06-01 --end-date 2026-06-03 --dry-run
PYTHONPATH=.:services/shared python -m platform_cli --pretty replay dlq --event-id evt_123 --dry-run
```
