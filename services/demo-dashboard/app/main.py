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

SERVICE_NAME = "demo-dashboard"
settings = service_settings(SERVICE_NAME)
configure_logging(SERVICE_NAME, settings.log_level)
request_metrics = InMemoryServiceMetrics()
postgres = Postgres(settings.database_url)

app = FastAPI(
    title="Data Platform Demo Dashboard",
    description="Lightweight tenant dashboard for metrics, alerts, pipeline status, and quality signals.",
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
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


async def dashboard_payload(tenant_id: str) -> dict[str, Any]:
    # This local dashboard is intentionally unauthenticated: a caller can select
    # any initialized tenant. Each protected-table request is constrained at
    # the PostgreSQL RLS layer to the selected tenant in addition to the
    # service's WHERE clause. Every query
    # against an RLS-protected table (tenant_metrics_daily, alerts,
    # service_health_metrics) goes through Postgres.fetch_scoped()/
    # fetchrow_scoped(), which sets app.tenant_id transaction-locally
    # before running, and the service now connects as the non-superuser,
    # non-bypass platform_tenant_scoped role in Docker Compose. The raw
    # Kubernetes manifests and Helm chart do not package this service.
    # tenant_config,
    # data_quality_score_daily, pipeline_run_log, and benchmark_run_results
    # are not RLS-protected tables (not tenant-scoped security data) and
    # intentionally stay on the plain, unscoped path.
    tenant = await postgres.fetchrow(
        """
        select tenant_id, tenant_name, plan, region
        from tenant_config
        where tenant_id = $1
        """,
        tenant_id,
    ) or {"tenant_id": tenant_id, "tenant_name": tenant_id, "plan": "unknown", "region": "unknown"}

    metrics = await postgres.fetch_scoped(
        tenant_id,
        """
        select metric_date, net_revenue, order_count, active_users,
               payment_success_count, payment_failure_count, events_processed
        from tenant_metrics_daily
        where tenant_id = $1
        order by metric_date desc
        limit 14
        """,
        tenant_id,
    )
    alerts = await postgres.fetch_scoped(
        tenant_id,
        """
        select alert_type, severity, status, message, created_at
        from alerts
        where tenant_id = $1
        order by created_at desc
        limit 8
        """,
        tenant_id,
    )
    quality = await postgres.fetchrow(
        """
        select quality_score, passed_checks, failed_checks, warning_checks,
               critical_checks, updated_at
        from data_quality_score_daily
        where tenant_id = $1
        order by score_date desc
        limit 1
        """,
        tenant_id,
    )
    pipelines = await postgres.fetch(
        """
        select pipeline_name, status, records_processed, started_at, finished_at
        from pipeline_run_log
        order by started_at desc
        limit 8
        """
    )
    service_health = await postgres.fetch_scoped(
        tenant_id,
        """
        select distinct on (service_name) service_name, status, latency_ms, error_count,
               throughput_per_minute, kafka_lag, event_timestamp
        from service_health_metrics
        where tenant_id = $1
        order by service_name, event_timestamp desc
        """,
        tenant_id,
    )
    benchmarks = await postgres.fetch(
        """
        select benchmark_name, total_events, events_per_second, failure_count,
               p95_latency_ms, created_at
        from benchmark_run_results
        where tenant_id = $1
        order by created_at desc
        limit 5
        """,
        tenant_id,
    )
    return {
        "tenant": tenant,
        "metrics": metrics,
        "alerts": alerts,
        "quality": quality,
        "pipelines": pipelines,
        "service_health": service_health,
        "benchmarks": benchmarks,
    }


@app.get("/api/dashboard")
async def dashboard_api(tenant_id: str = Query(default="tenant_demo")) -> dict[str, Any]:
    return await dashboard_payload(tenant_id)


@app.get("/", response_class=HTMLResponse)
async def dashboard(tenant_id: str = Query(default="tenant_demo")) -> HTMLResponse:
    payload = await dashboard_payload(tenant_id)
    return HTMLResponse(render_dashboard(payload))


def render_dashboard(payload: dict[str, Any]) -> str:
    tenant = payload["tenant"]
    latest_metric = payload["metrics"][0] if payload["metrics"] else {}
    quality = payload["quality"] or {}
    return f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(str(tenant["tenant_name"]))} | Data Platform</title>
  <style>
    :root {{
      --ink: #172026;
      --muted: #5d6970;
      --line: #d8dee2;
      --panel: #f7f9fa;
      --blue: #1f6feb;
      --green: #1a7f37;
      --red: #b42318;
      --amber: #9a6700;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: #ffffff;
    }}
    header {{
      border-bottom: 1px solid var(--line);
      padding: 20px 28px;
      display: flex;
      justify-content: space-between;
      gap: 18px;
      align-items: center;
    }}
    h1 {{ font-size: 22px; margin: 0; }}
    main {{ padding: 24px 28px 40px; }}
    .meta {{ color: var(--muted); font-size: 14px; margin-top: 4px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 22px;
    }}
    .panel {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      padding: 14px;
      min-height: 94px;
    }}
    .panel h2 {{ font-size: 13px; color: var(--muted); margin: 0 0 8px; font-weight: 600; }}
    .value {{ font-size: 24px; font-weight: 700; }}
    .section {{ margin-top: 24px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 10px 8px; text-align: left; }}
    th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0; }}
    .status-open, .status-failed {{ color: var(--red); font-weight: 700; }}
    .status-passed, .status-healthy, .status-complete {{ color: var(--green); font-weight: 700; }}
    .status-warning, .status-degraded {{ color: var(--amber); font-weight: 700; }}
    @media (max-width: 900px) {{
      header {{ align-items: flex-start; flex-direction: column; }}
      .grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    @media (max-width: 560px) {{
      main, header {{ padding-left: 16px; padding-right: 16px; }}
      .grid {{ grid-template-columns: 1fr; }}
      table {{ font-size: 12px; }}
    }}
  </style>
</head>
<body>
  <header>
    <div>
      <h1>{escape(str(tenant["tenant_name"]))}</h1>
      <div class="meta">Tenant {escape(str(tenant["tenant_id"]))} · {escape(str(tenant["plan"]))} · {escape(str(tenant["region"]))}</div>
    </div>
    <form method="get">
      <input name="tenant_id" value="{escape(str(tenant["tenant_id"]))}" aria-label="Tenant ID">
      <button type="submit">Load</button>
    </form>
  </header>
  <main>
    <div class="grid">
      {metric_card("Net revenue", currency(latest_metric.get("net_revenue")))}
      {metric_card("Orders", latest_metric.get("order_count", 0))}
      {metric_card("Active users", latest_metric.get("active_users", 0))}
      {metric_card("Quality score", quality.get("quality_score", "n/a"))}
    </div>
    {table_section("Daily metrics", payload["metrics"], ["metric_date", "net_revenue", "order_count", "active_users", "payment_failure_count", "events_processed"])}
    {table_section("Alerts", payload["alerts"], ["alert_type", "severity", "status", "message", "created_at"])}
    {table_section("Pipeline runs", payload["pipelines"], ["pipeline_name", "status", "records_processed", "started_at", "finished_at"])}
    {table_section("Service health", payload["service_health"], ["service_name", "status", "latency_ms", "error_count", "kafka_lag", "event_timestamp"])}
    {table_section("Benchmarks", payload["benchmarks"], ["benchmark_name", "total_events", "events_per_second", "failure_count", "p95_latency_ms", "created_at"])}
  </main>
</body>
</html>
"""


def metric_card(label: str, value: Any) -> str:
    return f'<section class="panel"><h2>{escape(label)}</h2><div class="value">{escape(str(value))}</div></section>'


def currency(value: Any) -> str:
    if value is None:
        return "$0"
    return f"${float(value):,.0f}"


def table_section(title: str, rows: list[dict[str, Any]], columns: list[str]) -> str:
    header = "".join(f"<th>{escape(column.replace('_', ' '))}</th>" for column in columns)
    body = "\n".join(table_row(row, columns) for row in rows)
    if not body:
        body = f'<tr><td colspan="{len(columns)}">No rows yet</td></tr>'
    return f"""
<section class="section">
  <h2>{escape(title)}</h2>
  <table>
    <thead><tr>{header}</tr></thead>
    <tbody>{body}</tbody>
  </table>
</section>
"""


def table_row(row: dict[str, Any], columns: list[str]) -> str:
    cells = []
    for column in columns:
        value = row.get(column, "")
        css_class = ""
        if column == "status":
            css_class = f' class="status-{escape(str(value).lower())}"'
        cells.append(f"<td{css_class}>{escape(str(value))}</td>")
    return f"<tr>{''.join(cells)}</tr>"
