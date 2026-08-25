from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
from platform_shared.schemas import EventType, idempotent_event_id, validate_event_payload


def derive_business_idempotency_key(*, source: str, entity_id: str, action: str) -> str:
    return f"{source}:{entity_id}:{action}".strip().lower()


@dataclass(frozen=True)
class ProducerEvent:
    tenant_id: str
    event_type: EventType | str
    payload: dict[str, Any]
    source_service: str
    idempotency_key: str
    payload_version: int = 1
    event_timestamp: datetime | None = None
    trace_id: str | None = None

    def to_request(self) -> dict[str, Any]:
        event_type = EventType(self.event_type)
        validated_payload = validate_event_payload(event_type, self.payload)
        timestamp = self.event_timestamp or datetime.now(timezone.utc)
        return {
            "event_id": idempotent_event_id(
                tenant_id=self.tenant_id,
                event_type=event_type,
                source_service=self.source_service,
                idempotency_key=self.idempotency_key,
            ),
            "tenant_id": self.tenant_id,
            "event_type": event_type.value,
            "source_service": self.source_service,
            "idempotency_key": self.idempotency_key,
            "payload_version": self.payload_version,
            "event_timestamp": timestamp.isoformat(),
            "trace_id": self.trace_id,
            "payload": validated_payload,
        }


@dataclass(frozen=True)
class IngestionResult:
    accepted: int
    failed: int
    results: list[dict[str, Any]]

    @property
    def ok(self) -> bool:
        return self.failed == 0


class PlatformProducerClient:
    def __init__(
        self,
        *,
        base_url: str,
        tenant_id: str,
        user_id: str = "producer-sdk",
        jwt_token: str | None = None,
        timeout_seconds: float = 10,
        max_retries: int = 3,
        backoff_seconds: float = 0.25,
        client: httpx.Client | None = None,
    ) -> None:
        """`jwt_token`: a signed JWT (see `platform_shared.auth.create_access_token`)
        to send as `Authorization: Bearer <jwt_token>`. Required against
        the platform's default configuration — `AUTH_MODE=strict` is the
        secure code-level default (see docs/security.md) — without it,
        ingestion-service rejects every request with 401 rather than
        falling back to the `X-Tenant-ID`/`X-User-ID` headers below, which
        are only trusted when the target service explicitly opts into
        `AUTH_MODE=dev_compat` (local demo convenience only).
        """
        self.base_url = base_url.rstrip("/")
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.jwt_token = jwt_token
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> PlatformProducerClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def publish(self, event: ProducerEvent) -> dict[str, Any]:
        response = self._request_with_retries("POST", "/events", json=event.to_request())
        return response.json()

    def publish_batch(self, events: list[ProducerEvent]) -> IngestionResult:
        payload = {"events": [event.to_request() for event in events]}
        response = self._request_with_retries("POST", "/events/batch", json=payload)
        body = response.json()
        return IngestionResult(
            accepted=int(body.get("accepted", 0)),
            failed=int(body.get("failed", 0)),
            results=list(body.get("results", [])),
        )

    def _headers(self) -> dict[str, str]:
        if self.jwt_token:
            return {"Authorization": f"Bearer {self.jwt_token}"}
        return {"X-Tenant-ID": self.tenant_id, "X-User-ID": self.user_id}

    def _request_with_retries(self, method: str, path: str, *, json: dict[str, Any]) -> httpx.Response:
        headers = self._headers()
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._client.request(
                    method,
                    f"{self.base_url}{path}",
                    headers=headers,
                    json=json,
                )
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                # A 4xx means the server has already told us definitively
                # why the request is bad (invalid payload, unauthorized,
                # forbidden, ...) — retrying identical bytes cannot change
                # that outcome. Only retry on 5xx (server-side, plausibly
                # transient) and TransportError below (network-level,
                # plausibly transient). Retrying a 401/422 with backoff
                # used to waste up to `max_retries` round trips masking a
                # deterministic failure as if it might eventually succeed.
                if exc.response.status_code < 500:
                    raise
                last_error = exc
                if attempt == self.max_retries:
                    break
                time.sleep(self.backoff_seconds * attempt)
            except httpx.TransportError as exc:
                last_error = exc
                if attempt == self.max_retries:
                    break
                time.sleep(self.backoff_seconds * attempt)
        assert last_error is not None
        raise last_error
