# Python Producer SDK Changelog

This SDK is small and internal (see `README.md`), but it's still an API
contract other teams' code imports directly — changes to
`PlatformProducerClient`'s constructor or retry behavior are breaking
changes for existing users. They are documented here from this release
forward.

## 0.2.0 — Authentication, retries, and validation

**Added**

- `PlatformProducerClient(..., jwt_token="...")` — send
  `Authorization: Bearer <jwt_token>` instead of the unsigned
  `X-Tenant-ID`/`X-User-ID` headers. **Required if the target service runs
  with `AUTH_MODE=strict`** (see `docs/security.md`) — before
  this release, the SDK had no way to authenticate against a service in
  strict mode at all and every request would fail with `401`.

**Fixed**

- The SDK previously retried *every* `httpx.HTTPStatusError`, including
  4xx client errors (bad payload → `422`, missing/invalid auth → `401`)
  — for up to `max_retries` attempts with backoff, before finally raising.
  A 4xx response is the server telling you definitively why the request
  is bad; retrying identical request bytes cannot change that outcome, and
  the retries only added latency and log noise ahead of an error that was
  never going to succeed. Now only 5xx responses and transport-level
  errors (`httpx.TransportError` — connection refused, timeout, DNS
  failure) are retried; any 4xx raises immediately on the first attempt.

## 0.1.0 (initial baseline)

Initial internal producer SDK: typed `ProducerEvent`/`PlatformProducerClient`,
payload validation before publish, deterministic idempotency keys,
single-event and batch publish helpers, retry/backoff around HTTP failures
(unconditional — see the 0.2.0 fix above).
