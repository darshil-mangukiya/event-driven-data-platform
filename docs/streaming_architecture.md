# Structured Streaming Architecture

`spark/streaming/` is an analytical Kafka-to-PostgreSQL path for event-time
windows, watermarks, deduplication, late-event handling, and streaming
telemetry. The asynchronous processing service remains the writer for
`processed_*` and `tenant_metrics_*`; Structured Streaming writes separate
`stream_*` tables.

## Pipeline

```text
Kafka domain topics
  -> parse envelope and payload
  -> validate event type, version, tenant, and domain fields
  -> route invalid records to DLQ
  -> watermark on event_timestamp
  -> deduplicate by (tenant_id, event_id)
  -> classify on_time / late_accepted / late_rejected
  -> broadcast-join tenant configuration
  -> aggregate five-minute tenant/domain windows
  -> idempotent PostgreSQL upsert
```

| Query | Input | Output |
| --- | --- | --- |
| `cloudscale-stream-aggregates` | Valid, deduplicated, accepted events | `stream_window_metrics`, checkpoint and late-event audit |
| `cloudscale-stream-dlq` | Invalid or late-rejected events | Kafka DLQ and late-event audit |
| `cloudscale-stream-dedup-audit` | Valid records before stateful dedup | Same-batch duplicate audit |
| `cloudscale-stream-received-metrics` | Raw topic records | Per-topic throughput metrics |

Each query has its own checkpoint directory.

## Event time and late data

The pipeline preserves producer `event_timestamp`, Kafka ingest time, and
processing time as separate fields.

- At or below `STREAMING_LATE_ACCEPT_THRESHOLD_SECONDS` (default 60):
  `on_time`.
- Between the accept threshold and
  `STREAMING_LATE_REJECT_THRESHOLD_SECONDS` (default 600):
  `late_accepted` and included in its window.
- Beyond the reject threshold: `late_rejected`, excluded from aggregates,
  and written to the DLQ and audit table.

Configuration requires the reject threshold to be no greater than the Spark
watermark delay so accepted events remain within retained state.

## Deduplication and checkpointing

`dropDuplicatesWithinWatermark(["tenant_id", "event_id"])` bounds state by
the watermark and keeps identical IDs independent across tenants. A lightweight
audit query counts same-micro-batch duplicates; later-batch duplicates are
suppressed but are not separately counted.

Checkpoint commits occur after a `foreachBatch` sink completes. A sink error
leaves offsets uncommitted, so the batch is retried. Deleting checkpoints
forces a restart from `STREAMING_STARTING_OFFSETS`; local checkpoints use a
Docker volume, while a deployed system would use durable object storage.

## Enrichment and aggregation

Tenant configuration is broadcast at job startup. Changes require a restart
to refresh the broadcast value. Product metadata remains a batch concern.

Five-minute windows group by tenant and event domain and produce:

- revenue;
- order count and units sold;
- payment success and failure counts;
- event count.

Rows are stored in long format as `metric_name` and `metric_value`.

## PostgreSQL sink

`PostgresSink` uses natural-key `ON CONFLICT` upserts and bounded retries.
After retry exhaustion it attempts to write `streaming_failures`, raises the
error, and prevents checkpoint advancement. PostgreSQL remains the serving
store; this path does not introduce another database.

The optional streaming profile uses `platform_admin_bypass` because Spark
JDBC batch writes do not set transaction-local tenant context. Tenant identity
still participates in deduplication, grouping, and serving keys.

## Validation status

- Parser, validation, watermark, deduplication, enrichment, aggregation, DLQ,
  and sink behavior are covered by `tests/streaming/`.
- Stateful watermark, deduplication, window, and checkpoint behavior execute
  with Spark's local rate source.
- Compose configuration for the streaming profile validates.
- Kafka-to-Spark-to-PostgreSQL container execution remains environment-limited;
  see `LIMITATIONS.md`.

## Observability

`spark/streaming/metrics.py` exports throughput, processing and watermark
lag, sink health, checkpoint age, validation failures, DLQ volume, late-event
counts, and duplicate counts. Prometheus rules cover lag, DLQ growth, sink
failures, checkpoint staleness, PostgreSQL availability, and validation or
late-event rates. See `OBSERVABILITY.md` and `slo-catalog.md`.

## Adding a contract

1. Add the payload model and JSON Schema.
2. Add its domain mapping and required fields in
   `spark/streaming/schemas.py`.
3. Add new supported payload versions to
   `spark/streaming/config.py::SUPPORTED_PAYLOAD_VERSIONS`.

Unknown versions route to the DLQ with
`unsupported_payload_version`.
