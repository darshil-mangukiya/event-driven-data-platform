# Python Producer SDK

The SDK gives event producers a typed path into the ingestion service.

Features:

- payload validation before HTTP publish
- deterministic idempotency keys
- retry/backoff around *transient* HTTP failures only (5xx and network
  errors — a 4xx client error, e.g. invalid payload or unauthorized, raises
  immediately instead of retrying a request that can't succeed)
- single-event and batch publish helpers
- `Authorization: Bearer <jwt>` support (`jwt_token=...`), required if the
  target service runs with `AUTH_MODE=strict` — see
  [../../docs/security.md](../../docs/security.md)

See [CHANGELOG.md](CHANGELOG.md) for version history; this is a versioned
API contract other teams' code imports directly, even though it's small.

Example:

```bash
PYTHONPATH=services/shared:sdk/python python sdk/python/examples/order_producer.py
```

Against a service running `AUTH_MODE=strict`, pass a signed JWT instead of
relying on the default tenant/user headers:

```python
from platform_shared.auth import TenantPrincipal, create_access_token

token = create_access_token(TenantPrincipal(user_id="checkout-api", tenant_id="tenant_demo", role="service_account"))
client = PlatformProducerClient(base_url="http://localhost:8001", tenant_id="tenant_demo", jwt_token=token)
```

This SDK is intentionally small. Production teams would publish it as an internal package and add tracing headers and richer producer telemetry.
