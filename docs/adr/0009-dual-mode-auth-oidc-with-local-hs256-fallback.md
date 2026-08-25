# ADR 0009: Dual-Mode Auth — Real OIDC/JWKS With a Local HS256 Fallback

## Status

Accepted

## Context

The platform already had a working local JWT auth mode (HS256,
locally-issued tokens) covering strict tenant-isolation enforcement
(`AUTH_MODE=strict`). the corresponding verification needed real OIDC/JWKS verification (RS256,
an external identity provider, JWKS rotation) without breaking the
existing local-dev/CI path, which does not have — and should not require
— a running identity provider just to run the test suite.

## Decision

Add `platform_shared.oidc` (`JWKSClient`, `verify_oidc_token`,
`oidc_claims_to_principal_kwargs`) as an additional, separate
verification path. `principal_from_authorization()` inspects the JWT
header: if it carries a `kid` and OIDC configuration
(`OIDC_JWKS_URL`/`OIDC_ISSUER`/`OIDC_AUDIENCE`) is present, it routes to
OIDC/JWKS RS256 verification against a real JWKS endpoint (live-verified
against a real local Keycloak instance); otherwise it falls back to the
existing local HS256 path unchanged. Both paths produce the same
`Principal` shape and go through the same tenant-scoping logic
downstream.

## Consequences

Local development and CI keep working exactly as before with zero new
infrastructure requirement — OIDC is additive, not a replacement that
would force every environment to run an identity provider. Production
deployments can point at a real IdP by setting the OIDC env vars, with no
code branching outside `principal_from_authorization()`. The tradeoff is
two code paths to keep correct instead of one; this was judged acceptable
because they share the same downstream `Principal`/tenant-scoping
contract and both are covered by their own test suites
(`tests/test_oidc.py`, the existing local-auth tests).
