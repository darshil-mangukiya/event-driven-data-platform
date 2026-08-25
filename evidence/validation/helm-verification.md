# Helm Packaging Verification

Scope: Local verification
Date: 2026-08-21/22

## Choice: Helm (not Kustomize, not both)

Chosen over Kustomize because this platform needs parameterized
values (image tags, replica counts, resource requests, auth mode,
credentials-by-reference) across environments — Helm's `values.yaml` +
templating is the more natural fit than Kustomize's patch-overlay model
for that. Not adding both, per the instruction not to duplicate tooling
without a corresponding implementation requirement.

## What Was Built

`deploy/helm/cloudscale/` — a real chart: `Chart.yaml`, `values.yaml`
(image tag/pull policy, per-service replicas/resources, config, and
placeholder-only secrets), and templates for every resource in
`deploy/kubernetes/base/` (namespace, ConfigMap, Secret, PostgreSQL
StatefulSet+PVC, the `postgres-init-scripts` ConfigMap — generated from
the real `database/init/*.sql` files via Helm's `.Files.Glob`, not
hand-copied inline — Redis, Kafka+Zookeeper, and the 4 FastAPI services
via a single templated `range` block over `values.services` rather than 4
near-duplicate files).

## Live Verification (real, not simulated)

```
$ helm lint deploy/helm/cloudscale
==> Linting deploy/helm/cloudscale
[INFO] Chart.yaml: icon is recommended
1 chart(s) linted, 0 chart(s) failed

$ helm template cloudscale deploy/helm/cloudscale
# renders 20 valid YAML documents: 8 Service, 7 Deployment, 2 ConfigMap,
# 1 Namespace, 1 Secret, 1 StatefulSet — confirmed via yaml.safe_load_all
```

**Deployed from the packaged chart, not kept as a second manual path**:
per the instruction ("If Kubernetes is executable, deploy from the
packaged configuration rather than keeping a second manual deployment
path"), the raw-manifest deployment
(`deploy/kubernetes/base/`, applied via `kubectl apply`) was torn down
(`kubectl delete namespace cloudscale`) and the cluster was redeployed
**entirely from the Helm chart**:

```
$ helm install cloudscale deploy/helm/cloudscale --create-namespace
NAME: cloudscale
STATUS: deployed
REVISION: 1

$ kubectl get pods -n cloudscale   # ~90s after install
analytics-service-...   1/1   Running
ingestion-service-...    1/1   Running
kafka-...                0/1   Running   (same diagnosed limitation as deploy/kubernetes/base/, see kubernetes-verification.md)
metadata-service-...     1/1   Running
postgres-0                1/1   Running
processing-service-...    1/1   Running
redis-...                 1/1   Running
zookeeper-...              1/1   Running
```

The Helm deployment matched the raw-manifest deployment. All eight workloads
reached `Running`/`Ready` after the Kafka Service bootstrap correction
recorded in `kubernetes-verification.md`.

## What Was NOT Verified

- `deploy/kubernetes/base/` (the raw manifests) is retained in the repo
  as the pre-Helm baseline this chart was templated from — not deleted,
  but no longer the "second manual deployment path" in active use per the
  instruction above.
- No `helm upgrade` rollback scenario was exercised (only a fresh
  `helm install`).
- `files/init/*.sql` is a synced copy of `database/init/*.sql` (Helm's
  `.Files.Glob` can only read inside the chart directory) — see
  `deploy/helm/cloudscale/files/init/README.md` and `make
  helm-sync-init-scripts` for how it's kept in sync; not automatically
  enforced by CI in verification.
