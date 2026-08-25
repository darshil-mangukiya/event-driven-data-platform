# Spark Structured Streaming Runtime

Status: **EXECUTED AND VERIFIED**.

The containerized job launched all five configured queries against Kafka,
classified on-time, out-of-order, duplicate, within-watermark, and
beyond-watermark events, wrote 12 window metric rows, exposed Spark metrics,
and used idempotent PostgreSQL upserts. Observed counters included 7 received,
5 processed, 1 duplicate, and 1 late-accepted event for the controlled batch;
the beyond-watermark event was routed to the DLQ.

The first launch exposed dependency, checkpoint-permission, watermark-default,
and Spark optimizer defects. After fixing those causes, the job restarted from
the persisted checkpoint and consumed later offsets 10 through 16 without
resetting prior state. This is bounded checkpoint/restart evidence, not disaster
recovery or a global exactly-once guarantee.
