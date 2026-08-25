# Retry, DLQ, and Replay

Status: **EXECUTED AND VERIFIED**, with the boundary below.

The Spark event `stream-late-rejected` arrived 736 seconds late, exceeded the
600-second watermark, and was written to the DLQ at partition 0, offset 30.
The operator tool normalized the Spark DLQ envelope, identified
`event_time_watermark_exceeded` at stage `spark-streaming`, corrected the event
timestamp, published the replay to `platform.events.orders`, and recorded one
replay audit row with reason `corrected-late-event-time`.

The replay command completed with `replayed=1`, `failed=0`. A second durable
business effect is deliberately not claimed: the original business key already
existed and the global uniqueness guard prevented duplication. Unit/integration
tests cover transient retry exhaustion and invalid-schema paths; this bounded
runtime specifically proves late-event DLQ inspection and operator replay.
