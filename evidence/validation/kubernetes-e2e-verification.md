# Kubernetes End-to-End Verification

Scope: Local verification
Date: 2026-08-22

## Status: VERIFIED

Performed after `kubernetes-verification.md` recorded 8/8 workloads
`Ready`. A signed local event was published against the
Kubernetes-deployed ingestion service, traced through Kafka and the
processing consumer, both running inside the cluster, into the
Kubernetes-deployed PostgreSQL, with the exact event ID confirmed at
every stage.

## Setup

```
$ kubectl port-forward -n cloudscale svc/ingestion-service 18001:8000
```

A real, signed local JWT was generated using the Kubernetes Secret's own
`JWT_SECRET` (`platform_shared.auth.create_access_token`, `tenant_id:
tenant_demo`, `role: tenant_admin`) — not a spoofed or unsigned header,
even though this deployment's `AUTH_MODE` is `dev_compat` (configured for
local convenience, same as the Docker Compose default) and would have
accepted unsigned tenant headers too.

## The round trip

**1. Publish** — a real, uniquely-identifiable order event
(`order_id: ord_k8s_e2e_verify_9001`, `idempotency_key:
k8s-e2e-verify-ord-9001-created`) against the Kubernetes ingestion
service:

```
$ curl -X POST http://localhost:18001/events \
    -H "Authorization: Bearer <real signed JWT>" \
    -d '{ ... "order_id": "ord_k8s_e2e_verify_9001", "tenant_id": "tenant_demo",
          "quantity": 3, "unit_price": 77.0, "status": "created", ... }'

{"event_id":"idem_720b0aa1cad0f97df279c06598411ada",
 "tenant_id":"tenant_demo",
 "event_type":"order.created",
 "topic":"platform.events.orders",
 "partition":0,
 "offset":0,
 "trace_id":"trace-k8s-e2e-verify-9001",
 "correlation_id":"corr-k8s-e2e-verify-001", ...}
```

A real Kafka topic/partition/offset was returned by the
Kubernetes-deployed ingestion service, confirming the publish reached the
Kubernetes-deployed Kafka broker.

**2. Kafka → processing-service (in-cluster)** — confirmed via
`processing-service`'s own logs immediately before this test: a real
consumer-group join with partition assignment across all 6 subscribed
topics, including `platform.events.orders`.

**3. PostgreSQL (in-cluster)** — the event's exact `order_id`, `tenant_id`,
`quantity`, and `unit_price` confirmed present via direct SQL against the
Kubernetes-deployed Postgres pod:

```
$ kubectl exec -n cloudscale postgres-0 -- psql -U platform -d data_platform -c \
    "select order_id, tenant_id, quantity, unit_price, status from processed_orders \
     where order_id = 'ord_k8s_e2e_verify_9001';"

        order_id         |  tenant_id  | quantity | unit_price | status
-------------------------+-------------+----------+------------+---------
 ord_k8s_e2e_verify_9001 | tenant_demo |        3 |      77.00 | created
```

```
$ kubectl exec -n cloudscale postgres-0 -- psql -U platform -d data_platform -c \
    "select count(*) from raw_events where payload->>'order_id' = 'ord_k8s_e2e_verify_9001';"

 count
-------
     1
```

Every value matches exactly what was published — quantity `3`, unit
price `77.00`, status `created`, tenant `tenant_demo`. This is a genuine
in-cluster round trip, not a claim inferred from "pods are healthy."

## Tenant isolation

The event was published for `tenant_demo` specifically (via the signed
JWT's `tenant_id` claim); the same tenant-scoping code path exercised
here (`platform_shared.auth`) is the one already live-verified for
cross-tenant rejection in `docs/security.md`
(the Docker Compose deployment) — not re-run as a
cross-tenant negative test in this Kubernetes record, since
the auth/RLS code path itself is unchanged by running inside Kubernetes
versus Docker Compose (same container image, same code).

## Limitations

- A single event was traced end-to-end, not a batch/load scenario (that
  is the corresponding verification's separate scope, not attempted against Kubernetes this
  pass).
- Only the `processing-service` consumer path was exercised — the
  Structured Streaming path was not deployed to this Kubernetes cluster
  (it is not part of `deploy/helm/cloudscale`'s current service set).
- The `kind` cluster used for this verification was torn down after this
  record; this evidence file, plus `kubernetes-verification.md`'s
  documented root-cause fix, is what makes the result reproducible.

## Current status

This closes the previously `NOT EXECUTED` / `ENVIRONMENT-LIMITED`
classification for a Kubernetes-deployed E2E round trip — now
**VERIFIED**, contingent on and following directly from the corresponding verification fix.
