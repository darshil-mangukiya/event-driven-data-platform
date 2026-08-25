"""Tests for the SDK hardening in
sdk/python/platform_producer/client.py.

Two real gaps this closes:

1. `PlatformProducerClient` had no way to send a signed JWT at all — only
   the unsigned `X-Tenant-ID`/`X-User-ID` headers, which the platform's
   secure default (`AUTH_MODE=strict`, unset falls back to strict — see
   tests/test_auth_hardening.py) never trusts. Every SDK caller against a
   default-configured service would have failed with 401 with no way to
   fix it from the SDK's own API.
2. `_request_with_retries` retried every `httpx.HTTPStatusError`,
   including 4xx client errors — a `422` (invalid payload) or `401`
   (unauthorized) would be retried up to `max_retries` times with backoff
   before finally raising, wasting time on a deterministically-failing
   request. Only 5xx and transport-level errors should be retried.
"""

from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SDK_PATH = PROJECT_ROOT / "sdk" / "python"
if str(SDK_PATH) not in sys.path:
    sys.path.insert(0, str(SDK_PATH))

from platform_producer import (  # noqa: E402
    PlatformProducerClient,
    ProducerEvent,
    derive_business_idempotency_key,
)


def _sample_event() -> ProducerEvent:
    return ProducerEvent(
        tenant_id="tenant_demo",
        event_type="order.created",
        source_service="checkout-api",
        idempotency_key=derive_business_idempotency_key(source="checkout-api", entity_id="ord_1", action="created"),
        payload={
            "order_id": "ord_1",
            "customer_id": "cust_1",
            "product_id": "prod_001",
            "quantity": 1,
            "unit_price": 10.0,
            "discount_amount": 0,
            "status": "created",
            "channel": "web",
        },
    )


def test_client_sends_bearer_token_header_when_jwt_token_is_set() -> None:
    seen_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.update(request.headers)
        return httpx.Response(202, json={"event_id": "e1", "tenant_id": "tenant_demo", "event_type": "order.created", "topic": "t", "partition": 0, "offset": 1, "trace_id": None, "idempotency_key": "k"})

    client = PlatformProducerClient(
        base_url="http://ingestion.test",
        tenant_id="tenant_demo",
        jwt_token="a.fake.jwt",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    client.publish(_sample_event())

    assert seen_headers.get("authorization") == "Bearer a.fake.jwt"
    assert "x-tenant-id" not in seen_headers


def test_client_falls_back_to_x_headers_when_no_jwt_token_given() -> None:
    """Preserves the original X-header behavior for callers who haven't
    set jwt_token — no breaking change to the SDK's own request shape.
    Note this only actually authenticates against a service explicitly
    running AUTH_MODE=dev_compat; against the platform's secure default
    (AUTH_MODE=strict) these headers alone would be rejected with 401 (see
    tests/test_auth_hardening.py for that server-side behavior).
    """
    seen_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.update(request.headers)
        return httpx.Response(202, json={"event_id": "e1", "tenant_id": "tenant_demo", "event_type": "order.created", "topic": "t", "partition": 0, "offset": 1, "trace_id": None, "idempotency_key": "k"})

    client = PlatformProducerClient(
        base_url="http://ingestion.test",
        tenant_id="tenant_demo",
        user_id="my-service",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    client.publish(_sample_event())

    assert seen_headers.get("x-tenant-id") == "tenant_demo"
    assert seen_headers.get("x-user-id") == "my-service"
    assert "authorization" not in seen_headers


def test_client_does_not_retry_a_422_validation_error() -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(422, json={"detail": "invalid payload"})

    client = PlatformProducerClient(
        base_url="http://ingestion.test",
        tenant_id="tenant_demo",
        max_retries=3,
        backoff_seconds=0.001,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        client.publish(_sample_event())

    assert exc_info.value.response.status_code == 422
    assert call_count == 1, f"expected exactly 1 attempt for a 422, got {call_count}"


def test_client_does_not_retry_a_401_unauthorized() -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(401, json={"detail": "unauthorized"})

    client = PlatformProducerClient(
        base_url="http://ingestion.test",
        tenant_id="tenant_demo",
        max_retries=3,
        backoff_seconds=0.001,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    with pytest.raises(httpx.HTTPStatusError):
        client.publish(_sample_event())

    assert call_count == 1


def test_client_retries_a_503_up_to_max_retries_then_raises() -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(503, json={"detail": "unavailable"})

    client = PlatformProducerClient(
        base_url="http://ingestion.test",
        tenant_id="tenant_demo",
        max_retries=3,
        backoff_seconds=0.001,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        client.publish(_sample_event())

    assert exc_info.value.response.status_code == 503
    assert call_count == 3, f"expected all 3 attempts to be used for a 503, got {call_count}"


def test_client_succeeds_after_a_transient_503_then_a_200() -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(503, json={"detail": "unavailable"})
        payload = __import__("json").loads(request.content)
        return httpx.Response(
            202,
            json={
                "event_id": payload["event_id"],
                "tenant_id": payload["tenant_id"],
                "event_type": payload["event_type"],
                "topic": "t",
                "partition": 0,
                "offset": 1,
                "trace_id": None,
                "idempotency_key": payload["idempotency_key"],
            },
        )

    client = PlatformProducerClient(
        base_url="http://ingestion.test",
        tenant_id="tenant_demo",
        max_retries=3,
        backoff_seconds=0.001,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    result = client.publish(_sample_event())

    assert call_count == 2
    assert result["event_id"]


def test_sdk_exports_a_version() -> None:
    import platform_producer

    assert platform_producer.__version__ == "0.2.0"
