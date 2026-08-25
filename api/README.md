# API Contracts

This folder contains API review artifacts:

| Path | Purpose |
| --- | --- |
| `platform-api.http` | Hand-run REST examples for local services. |
| `openapi/*.openapi.json` | Exported FastAPI OpenAPI contracts. |
| `fixtures/*.json` | Example request/response payloads for producers and consumers. |

Refresh generated contracts:

```bash
PYTHONPATH=services/shared:services/processing-service python scripts/export_openapi_contracts.py
```
