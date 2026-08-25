# Tenant Onboarding

The project includes an operator workflow for adding a new tenant to the local platform.

## CLI Command

```bash
PYTHONPATH=.:services/shared python -m platform_cli --pretty tenant create \
  --tenant-id tenant_newco \
  --tenant-name "NewCo Analytics" \
  --plan growth \
  --region us \
  --output-events data/onboarding/tenant_newco_events.jsonl \
  --dry-run
```

Remove `--dry-run` when PostgreSQL is running through Docker Compose.

## What It Creates

- `tenant_config` record.
- Tenant admin, analyst, viewer, and service account users.
- Local JWT token for the service account.
- Seed product metadata.
- Traceable sample events with `trace_id`, `correlation_id`, `causation_id`, and `idempotency_key`.
- A starter `tenant_metrics_daily` row so analytics APIs can return tenant-scoped output immediately.
- Readiness checks for config, users, serving metrics, and traceable raw events.

## Validate Readiness

```bash
PYTHONPATH=.:services/shared python -m platform_cli --pretty tenant validate \
  --tenant-id tenant_newco \
  --dry-run
```

The dry-run prints the exact SQL checks. A live run executes them against PostgreSQL.

## Production Hardening Path

In production, onboarding would be approved through a platform admin workflow, secrets would be issued through a secrets manager, tenant users would sync from identity providers, and seed events would be sent through the ingestion API rather than inserted directly for local convenience.
