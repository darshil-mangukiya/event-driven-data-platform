# OpenAPI Contracts

Service contracts are exportable as JSON artifacts under `api/openapi/`.

Export them with:

```bash
PYTHONPATH=services/shared:services/processing-service python scripts/export_openapi_contracts.py
```

The export also writes sample request/response fixtures to `api/fixtures/`.

## Why Export Contracts

- Reviewers can inspect APIs without running Docker.
- Client teams can generate internal SDKs.
- Contract diffs can be reviewed before service changes.
- Fixtures document tenant headers, idempotency keys, and response shapes.

## Contract Ownership

| Service | Owner | Contract risk |
| --- | --- | --- |
| Ingestion | Data platform | Producer compatibility and idempotency behavior. |
| Processing | Data platform | Health/status surface and operational endpoints. |
| Analytics | Analytics platform | Metric semantics, pagination, tenant scope, cache behavior. |
| Metadata | Platform governance | Tenant metadata, RBAC scaffold, token issue flow. |
| Demo Dashboard | Data platform | Local demo surface only. |
