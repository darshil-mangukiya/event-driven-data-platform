# Ops Console

The ops console is a read-only FastAPI/HTML operator surface at port `8006` in
Docker Compose. Authentication is not configured, and Docker Compose publishes
the interface on loopback by default. Shared deployments require administrative
authentication and network restrictions.

Local URL:

```text
http://localhost:8006/?tenant_id=tenant_demo
```

## Sections

The page is organized into three groups:

### Tenants & Platform (pre-existing)

- Tenants
- Service health
- Outbox / Inbox status
- Reconciliation audit rows
- Lineage events
- Incidents / alerts
- Privacy erasure requests
- Pipeline runs

### Structured Streaming

- **Streaming runs** — recent `stream_processing_runs` rows (job status, started/ended)
- **Checkpoint freshness (by query)** — last commit per streaming query from `streaming_checkpoint_audit`, independent of whether the streaming job's own `/metrics` endpoint (`spark/streaming/metrics.py`, port 8007) is reachable
- **Streaming failures** — recent `streaming_failures` rows (sink write failures after retries exhausted)
- **Late events (last 24h, by classification)** — counts from `streaming_late_events`

### Reliability & Observability

- **Reliability exercise results** — the most recent outcome per scenario, read from `pipeline_run_log` (written by `reliability/runner.py`; see `docs/reliability.md` "Reliability → Detection Mapping")
- **Serving freshness (by table)** — staleness of `tenant_metrics_daily` and `stream_window_metrics`, the same signal exposed as `cloudscale_serving_metrics_staleness_seconds`
- Direct links to Prometheus (`:9090`), Grafana (`:3000`), and this service's own `/metrics`

### Data Products

- **Registered data products** — the 6 data products from `contracts/data_products/registry.yml`: name, domain, modeled consumers, API endpoint, freshness target, tenant scope, and status. This is static contract metadata, not a live query — see `docs/data-products.md` for the full narrative and `docs/consumer-requirements.md` for the modeled-consumer disclaimer.

## Design: one query, two consumers

The Streaming and Reliability sections above don't duplicate SQL. Both the ops console's HTML page and this service's `/metrics` Prometheus endpoint (`app/observability.py`) call the same `fetch_*` functions — `fetch_checkpoint_freshness`, `fetch_reliability_status`, `fetch_serving_freshness`, `fetch_late_events_summary` — so the numbers an operator sees in the browser and the numbers Prometheus/Grafana show can never drift apart from two independently-maintained queries.

## Resilience

The Data Products section degrades to an empty table rather than crashing the whole page if `contracts/data_products/registry.yml` is unreadable (e.g. a container image built previously) — matching `app/observability.py::refresh_all`'s existing per-signal isolation, where one failing signal never blocks the others or the page/scrape itself.

## API

`GET /api/ops?tenant_id=...` returns the same payload as JSON, including all
console sections, for programmatic access.
