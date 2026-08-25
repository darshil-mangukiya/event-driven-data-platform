# Release Readiness

Run the platform preflight before a release or local demo:

```bash
PYTHONPATH=services/shared:services/processing-service python scripts/platform_preflight.py \
  --output-json evidence/validation/release-readiness.json \
  --output-md evidence/validation/release-readiness.md \
  --pretty
```

The preflight runs contract, catalog, sample, privacy, schema drift, RLS, metric contract, and resilience checks.

## Rollback Checklist

1. Confirm the failing service or migration.
2. Roll back application image first.
3. Avoid destructive down migrations unless tested.
4. Pause processing workers if data correctness is at risk.
5. Use replay/backfill/reconciliation after recovery.
6. Record incident notes when SLOs or data correctness were impacted.
