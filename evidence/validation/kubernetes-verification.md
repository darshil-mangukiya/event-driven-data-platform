# Kubernetes Execution Verification

Status: **VERIFIED — 8/8 workloads Ready**

Date: 2026-08-22

## Bootstrap correction

The single-broker Kafka deployment advertises its Service name during
initialization. Kubernetes normally omits not-ready pods from Service
endpoints, creating a cycle: Kafka needed its Service before its readiness
probe could pass.

`publishNotReadyAddresses: true` was added to the Kafka Service in the raw
manifest and Helm template. This setting is scoped to the current
single-broker local topology.

## Local kind result

```text
analytics-service    1/1 Running
ingestion-service    1/1 Running
kafka                1/1 Running
metadata-service     1/1 Running
postgres             1/1 Running
processing-service   1/1 Running
redis                1/1 Running
zookeeper            1/1 Running
```

The processing service joined its Kafka consumer group and received partition
assignments. Services were ClusterIP-only in namespace `cloudscale`.

The run used kind v0.32.0 with `kindest/node:v1.36.1`, locally built service
images, and the Helm chart under `deploy/helm/cloudscale`.

## Boundaries

- The Helm chart packages four application services; ops console, demo
  dashboard, and schema registry are outside that chart.
- KEDA lag connectivity is covered separately; scale-up/down was not observed.
- The bootstrap setting was exercised with one broker, not a rolling
  multi-broker deployment.
- The temporary kind cluster was removed after validation.
