# Consumer Onboarding

This is the internal consumer guide for teams using the analytics platform.

## Before You Start

1. Confirm your tenant ID and role.
2. Choose API access or downstream dbt/BI models.
3. Review metric ownership in `docs/metric-ownership.md`.
4. Review API contracts in `api/openapi/`.

## API Usage Pattern

```bash
curl -H "X-Tenant-ID: tenant_demo" \
  -H "X-User-ID: analyst_demo" \
  "http://localhost:8003/metrics/revenue?tenant_id=tenant_demo&limit=7"
```

## Consumer Responsibilities

- Always pass tenant context.
- Use pagination for large result sets.
- Cache downstream dashboards responsibly.
- Report metric mismatches with tenant/date examples.
- Do not join tenant-scoped data across tenants without platform-admin approval.

## Support Paths

| Need | Starting point |
| --- | --- |
| Metric definition | `docs/metric-ownership.md` |
| API contract | `docs/openapi-contracts.md` |
| Data correctness issue | `docs/reconciliation.md` |
| Latency issue | `docs/query-optimization.md` |
| New event producer | `docs/producer-sdk.md` |
