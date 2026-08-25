# KEDA Kafka-Lag Autoscaling Verification

Status: **CONFIGURATION ONLY**

Date: 2026-08-22

The KEDA operator and admission webhook ran in the `keda` namespace. The
`processing-service-kafka-lag` ScaledObject targeted:

- deployment: `processing-service`;
- topic: `platform.events.orders`;
- consumer group: `processing-service`;
- lag threshold: 50;
- activation threshold: 5;
- replica range: 1–5.

## Connectivity result

Kafka originally advertised the short hostname `kafka:9092`, which did not
resolve from KEDA's namespace. Both the raw manifest and Helm template now
advertise `kafka.cloudscale.svc.cluster.local:9092`.

After that change:

```text
ScaledObject READY=True ACTIVE=False
HPA target 0/50, replicas 1
```

KEDA could read consumer lag. Eighty queued order events were consumed by one
replica before a polling cycle observed lag above the activation threshold.
No scale-up or scale-down event was observed.

The topic had one partition, so replicas beyond one would not add consumer
parallelism. A scaling exercise first needs a partitioned topic and a backlog
that persists across polling intervals.
