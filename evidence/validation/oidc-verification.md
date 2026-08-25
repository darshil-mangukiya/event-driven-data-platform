# OIDC / JWKS Authentication Verification

Status: **VERIFIED**

Date: 2026-08-22

## Implementation

`platform_shared.oidc` provides:

- JWKS retrieval and `kid` caching;
- one refresh on an unknown `kid`;
- RS256-only signature verification;
- issuer, optional audience, and expiration validation;
- configurable tenant, role, and user claim mapping.

When OIDC configuration is present, a bearer token with a `kid` header uses
the OIDC path. Failure does not fall back to HS256. Without OIDC configuration,
the strict local HS256 path remains available.

## Local Keycloak results

The integration check used Keycloak 25.0 on localhost and its JWKS endpoint.

| Case | Result |
| --- | --- |
| Valid RS256 token | PASS |
| Tampered signature | REJECTED |
| Wrong issuer | REJECTED |
| Wrong audience | REJECTED |
| Expired token | REJECTED |
| Non-RS256 algorithm | REJECTED |
| Dispatch through `principal_from_authorization` | PASS |

Tenant and role defaults were used because the temporary realm did not define
custom claim mappers. A deployed IdP must map those claims explicitly.

`tests/test_oidc.py` also covers locally generated RSA keys, cache refresh,
claim mapping, HS256 fallback routing, and an optional live Keycloak check.

## Boundary

Key rotation is tested with changing JWKS fixtures, not by rotating a
Keycloak realm key. OIDC is optional and no provider credentials are stored in
the repository.
