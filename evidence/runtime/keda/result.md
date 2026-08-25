# KEDA Autoscaling Result

Status: **EXECUTED AND VERIFIED** locally with KEDA 2.20.2.

The operator, admission webhook, metrics API, CRDs, ScaledObject, and generated
HPA were healthy. A deterministic 20,000-event API load produced observed
consumer lag of 17,000. KEDA scaled processing from one replica to four and then
five; lag drained to zero, and the workload returned to one replica after the
30-second stabilization policy.

Replicas cannot outrun Kafka partition parallelism, PostgreSQL capacity,
network, API, or per-message processing costs. This is local lag-driven scaling
evidence, not production elasticity or infinite scalability.
