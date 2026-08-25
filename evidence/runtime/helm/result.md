# Helm Lifecycle Result

Status: **EXECUTED AND VERIFIED** locally.

`helm lint` and `helm template` passed. Release `p6-local` installed as revision
1, upgraded the processing Deployment to two replicas at revision 2, rolled
back to revision 1 as revision 3, then upgraded to revision 4 with the Kafka
topic bootstrap hook. Revision 4 was deployed and the hook Job completed.

Runtime validation exposed two packaging gaps now fixed: the chart omitted the
strict RLS initialization SQL, and Kafka started `orders` with one partition
instead of the declared 12. The chart now packages RLS initialization and
idempotently creates/reconciles all seven topics; `orders` reported 12
partitions and 604800000 ms retention. This was a safe configuration lifecycle,
not a destructive bad-image rollback exercise.
