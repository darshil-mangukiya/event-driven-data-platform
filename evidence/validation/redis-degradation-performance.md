# Redis Degradation Measurement

Status: **LOCALLY VERIFIED**

Date: 2026-08-22

`GET /metrics/revenue?tenant_id=tenant_demo` was measured while Redis was
warm, stopped, and restarted.

| Condition | Requests | Latency | Cache flag | HTTP |
| --- | ---: | --- | --- | ---: |
| Cold | 1 | 54.6 ms | — | 200 |
| Warm | 10 | 2.8–3.9 ms; mean 3.3 ms | `true` | 200 |
| Redis stopped | 10 | 15.5–22.1 ms; mean 18.4 ms | `false` | 200 |
| First request after restart | 1 | 15.6 ms | reconnecting | 200 |
| Following requests | 2 | 3.2–3.4 ms | `true` | 200 |

All outage requests returned complete PostgreSQL-backed responses. Cache
recovery required no analytics-service restart.

This is a single-endpoint, single-tenant, small-sample local measurement. It
does not provide percentile or production-load claims, and the rate-limiter
fallback path was not measured in the same run.
