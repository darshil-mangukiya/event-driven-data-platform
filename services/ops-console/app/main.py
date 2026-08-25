from __future__ import annotations

from html import escape
from typing import Any

from fastapi import FastAPI, Query
from platform_shared.config import service_settings
from platform_shared.database import Postgres
from platform_shared.logging import configure_logging
from platform_shared.metrics import InMemoryServiceMetrics, MetricsMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import HTMLResponse, Response

from app.observability import (
    fetch_checkpoint_freshness,
    fetch_late_events_summary,
    fetch_reliability_status,
    fetch_serving_freshness,
)
from app.observability import refresh_all as refresh_observability_gauges

# data_products/ lives at the repo root, not under services/ — importable
# here the same way platform_shared is: via PYTHONPATH, set by
# docker/Dockerfile.service's ENV in the container and by the project's
# `PYTHONPATH=.:services/shared` convention in local dev (see Makefile).
# No sys.path manipulation in application code, matching the rest of this
# service's imports.
from data_products.registry import load_registry as load_data_product_registry

SERVICE_NAME = "ops-console"
settings = service_settings(SERVICE_NAME)
configure_logging(SERVICE_NAME, settings.log_level)
request_metrics = InMemoryServiceMetrics()
postgres = Postgres(settings.database_url)

app = FastAPI(
    title="Data Platform Ops Console",
    description="Operator console for tenants, incidents, lineage, backfills, reconciliation, and service health.",
    version="0.1.0",
)
app.add_middleware(MetricsMiddleware, metrics=request_metrics, service_name=SERVICE_NAME)


@app.on_event("startup")
async def startup() -> None:
    await postgres.connect()


@app.on_event("shutdown")
async def shutdown() -> None:
    await postgres.close()


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"service": SERVICE_NAME, "status": "ok"}


@app.get("/metrics")
async def prometheus_metrics() -> Response:
    # Best-effort: refresh the DB-derived gauges (reliability exercise
    # outcomes, reconciliation health, checkpoint freshness, late-event
    # volume) right before this scrape. See app/observability.py for why
    # these are computed here rather than from a dedicated exporter.
    await refresh_observability_gauges(postgres)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def _data_products_summary() -> list[dict[str, Any]]:
    """Static contract metadata from the data-product registry —
    not a database query. Best-effort: a missing/unreadable registry file
    (e.g. a container image built before this feature) degrades to an
    empty list rather than breaking the whole console page, matching
    app/observability.py's refresh_all() per-signal isolation.
    """
    try:
        registry = load_data_product_registry()
    except Exception:  # noqa: BLE001
        return []
    return [
        {
            "name": p["name"],
            "domain": p["domain"],
            "modeled_consumers": ", ".join(p.get("modeled_consumers", [])),
            "api_endpoint": p.get("api_endpoint"),
            "freshness_target": p.get("freshness_target"),
            "tenant_scoped": p.get("tenant_isolation", {}).get("tenant_scoped"),
            "status": p.get("status"),
        }
        for p in registry.get("data_products", [])
    ]


async def ops_payload(tenant_id: str) -> dict[str, Any]:
    tenants = await postgres.fetch(
        """
        select tenant_id, tenant_name, plan, region, is_active
        from tenant_config
        order by tenant_id
        """
    )
    reconciliation = await postgres.fetch(
        """
        select tenant_id, metric_date, status, revenue_delta, order_count_delta,
               units_sold_delta, checked_at
        from reconciliation_audit
        where tenant_id = $1
        order by checked_at desc
        limit 10
        """,
        tenant_id,
    )
    lineage = await postgres.fetch(
        """
        select job_name, run_id, tenant_id, status, event_timestamp
        from lineage_events
        where tenant_id = $1 or tenant_id is null
        order by event_timestamp desc
        limit 10
        """,
        tenant_id,
    )
    outbox = await postgres.fetch(
        """
        select status, count(*) as event_count, max(updated_at) as latest_update
        from event_outbox
        group by status
        order by status
        """
    )
    inbox = await postgres.fetch(
        """
        select status, count(*) as event_count, max(received_at) as latest_received
        from event_inbox
        group by status
        order by status
        """
    )
    incidents = await postgres.fetch(
        """
        select alert_type, severity, status, message, created_at
        from alerts
        where tenant_id = $1
        order by created_at desc
        limit 10
        """,
        tenant_id,
    )
    privacy = await postgres.fetch(
        """
        select request_id, tenant_id, subject_type, status, requested_by, requested_at, completed_at
        from privacy_erasure_requests
        where tenant_id = $1
        order by requested_at desc
        limit 10
        """,
        tenant_id,
    )
    pipelines = await postgres.fetch(
        """
        select pipeline_name, status, records_processed, started_at, finished_at
        from pipeline_run_log
        order by started_at desc
        limit 10
        """
    )
    health_rows = await postgres.fetch(
        """
        select distinct on (service_name) service_name, status, latency_ms, error_count,
               kafka_lag, event_timestamp
        from service_health_metrics
        where tenant_id = $1
        order by service_name, event_timestamp desc
        """,
        tenant_id,
    )
    streaming_runs = await postgres.fetch(
        """
        select run_id, job_name, status, started_at, ended_at
        from stream_processing_runs
        order by started_at desc
        limit 5
        """
    )
    streaming_failures = await postgres.fetch(
        """
        select stage, tenant_id, error_message, recorded_at
        from streaming_failures
        order by recorded_at desc
        limit 10
        """
    )
    # These four reuse the exact same queries the /metrics endpoint's
    # Prometheus gauges are derived from (app/observability.py) rather than
    # maintaining a second copy of the SQL — see that module's docstring.
    streaming_checkpoints = await fetch_checkpoint_freshness(postgres)
    reliability_runs = await fetch_reliability_status(postgres)
    serving_freshness = await fetch_serving_freshness(postgres)
    late_events = await fetch_late_events_summary(postgres)
    return {
        "tenant_id": tenant_id,
        "tenants": tenants,
        "reconciliation": reconciliation,
        "lineage": lineage,
        "outbox": outbox,
        "inbox": inbox,
        "incidents": incidents,
        "privacy": privacy,
        "pipelines": pipelines,
        "health": health_rows,
        "streaming_runs": streaming_runs,
        "streaming_failures": streaming_failures,
        "streaming_checkpoints": streaming_checkpoints,
        "reliability_runs": reliability_runs,
        "serving_freshness": serving_freshness,
        "late_events": late_events,
        "data_products": _data_products_summary(),
    }


@app.get("/api/ops")
async def ops_api(tenant_id: str = Query(default="tenant_demo")) -> dict[str, Any]:
    return await ops_payload(tenant_id)


@app.get("/", response_class=HTMLResponse)
async def ops_console(tenant_id: str = Query(default="tenant_demo")) -> HTMLResponse:
    return HTMLResponse(render_ops(await ops_payload(tenant_id)))


def render_ops(payload: dict[str, Any]) -> str:
    tenant_id = payload["tenant_id"]
    return f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Platform Ops Console</title>
  <style>
    :root {{
      --ink: #172026;
      --muted: #5d6970;
      --line: #d8dee2;
      --panel: #f7f9fa;
      --green: #1a7f37;
      --red: #b42318;
      --amber: #9a6700;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: #fff;
    }}
    header {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: center;
      padding: 20px 28px;
      border-bottom: 1px solid var(--line);
    }}
    h1 {{ font-size: 22px; margin: 0; }}
    main {{ padding: 24px 28px 40px; }}
    .meta {{ color: var(--muted); margin-top: 4px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
      align-items: start;
    }}
    section {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      padding: 14px;
    }}
    h2 {{ font-size: 15px; margin: 0 0 10px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 8px 6px; text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0; }}
    .status-failed, .status-open {{ color: var(--red); font-weight: 700; }}
    .status-passed, .status-processed, .status-published, .status-healthy {{ color: var(--green); font-weight: 700; }}
    .status-pending, .status-publishing, .status-degraded {{ color: var(--amber); font-weight: 700; }}
    .status-true {{ color: var(--green); font-weight: 700; }}
    .status-false {{ color: var(--red); font-weight: 700; }}
    .group-title {{
      grid-column: 1 / -1;
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--muted);
      margin: 20px 0 -4px;
      padding-top: 12px;
      border-top: 1px solid var(--line);
    }}
    .group-title:first-of-type {{ border-top: none; padding-top: 0; margin-top: 0; }}
    .links {{ grid-column: 1 / -1; display: flex; gap: 12px; flex-wrap: wrap; }}
    .links a {{
      display: inline-block;
      padding: 8px 14px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel);
      color: var(--ink);
      text-decoration: none;
      font-size: 13px;
    }}
    .links a:hover {{ border-color: var(--muted); }}
    @media (max-width: 900px) {{ .grid {{ grid-template-columns: 1fr; }} header {{ align-items: flex-start; flex-direction: column; }} }}
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Platform Ops Console</h1>
      <div class="meta">Tenant scope: {escape(str(tenant_id))}</div>
    </div>
    <form method="get">
      <input name="tenant_id" value="{escape(str(tenant_id))}" aria-label="Tenant ID">
      <button type="submit">Load</button>
    </form>
  </header>
  <main class="grid">
    <div class="group-title">Tenants &amp; Platform</div>
    {table_section("Tenants", payload["tenants"], ["tenant_id", "tenant_name", "plan", "region", "is_active"])}
    {table_section("Service health", payload["health"], ["service_name", "status", "latency_ms", "error_count", "kafka_lag", "event_timestamp"])}
    {table_section("Outbox", payload["outbox"], ["status", "event_count", "latest_update"])}
    {table_section("Inbox", payload["inbox"], ["status", "event_count", "latest_received"])}
    {table_section("Reconciliation", payload["reconciliation"], ["metric_date", "status", "revenue_delta", "order_count_delta", "checked_at"])}
    {table_section("Lineage", payload["lineage"], ["job_name", "run_id", "tenant_id", "status", "event_timestamp"])}
    {table_section("Incidents", payload["incidents"], ["alert_type", "severity", "status", "message", "created_at"])}
    {table_section("Privacy requests", payload["privacy"], ["request_id", "subject_type", "status", "requested_by", "requested_at", "completed_at"])}
    {table_section("Pipeline runs", payload["pipelines"], ["pipeline_name", "status", "records_processed", "started_at", "finished_at"])}

    <div class="group-title">Structured Streaming</div>
    {table_section("Streaming runs", payload["streaming_runs"], ["run_id", "job_name", "status", "started_at", "ended_at"])}
    {table_section("Checkpoint freshness (by query)", payload["streaming_checkpoints"], ["query_name", "last_commit"])}
    {table_section("Streaming failures", payload["streaming_failures"], ["stage", "tenant_id", "error_message", "recorded_at"])}
    {table_section("Late events (last 24h, by classification)", payload["late_events"], ["classification", "recent_count"])}

    <div class="group-title">Reliability &amp; Observability</div>
    {table_section("Reliability exercise results (last run per scenario)", payload["reliability_runs"], ["scenario_id", "status", "finished_at"])}
    {table_section("Serving freshness (by table)", payload["serving_freshness"], ["table", "last_update", "staleness_seconds"])}
    <section class="links">
      <a href="http://localhost:9090" target="_blank" rel="noopener">Prometheus →</a>
      <a href="http://localhost:3000" target="_blank" rel="noopener">Grafana →</a>
      <a href="/metrics" target="_blank" rel="noopener">This service's /metrics →</a>
    </section>

    <div class="group-title">Data Products</div>
    {table_section("Registered data products", payload["data_products"], ["name", "domain", "modeled_consumers", "api_endpoint", "freshness_target", "tenant_scoped", "status"])}
  </main>
</body>
</html>
"""


def table_section(title: str, rows: list[dict[str, Any]], columns: list[str]) -> str:
    header = "".join(f"<th>{escape(column.replace('_', ' '))}</th>" for column in columns)
    body = "\n".join(table_row(row, columns) for row in rows)
    if not body:
        body = f'<tr><td colspan="{len(columns)}">No rows yet</td></tr>'
    return f"<section><h2>{escape(title)}</h2><table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table></section>"


STATUS_COLORED_COLUMNS = {"status", "tenant_scoped"}


def table_row(row: dict[str, Any], columns: list[str]) -> str:
    cells = []
    for column in columns:
        value = row.get(column, "")
        class_name = f"status-{str(value).lower()}" if column in STATUS_COLORED_COLUMNS else ""
        cells.append(f'<td class="{class_name}">{escape(str(value))}</td>')
    return "<tr>" + "".join(cells) + "</tr>"
