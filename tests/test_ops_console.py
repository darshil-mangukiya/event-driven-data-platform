"""Tests for the ops-console extension: streaming pipeline
status, reliability exercise results, serving freshness, and the
data-product registry surfaced in the operator HTML page.

Uses the same import-the-live-app + fake-Postgres pattern as
tests/test_api_contracts.py rather than a live database.
"""

from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _import_ops_console_main_fresh():
    """Import once and cache it for the rest of the process.

    Two things make this trickier than tests/test_api_contracts.py's
    per-call ``import_service_main``:

    1. ``app/observability.py`` registers module-level Prometheus Gauges on
       import; re-importing it would re-run those ``Gauge(...)`` definitions
       and collide with the already-registered timeseries.
    2. Deleting ``sys.modules["app"]`` and leaving it pointing at
       ops-console's ``app`` package would break any *other* test module
       that does a bare, module-level ``from app.X import ...`` at
       collection time (e.g. tests/test_processing_logic.py imports
       ``app.processors`` from processing-service via tests/conftest.py's
       sys.path setup) — Python would resolve ``app`` from the stale cache
       instead of re-resolving it via sys.path.

    So this helper is called lazily, from a fixture, the first time a test
    in this module actually runs — never at collection time (module import)
    — so it can never race against another test file's collection-time
    ``from app.X import ...``. It caches the result in a module-level
    variable on first call and returns the cached module on every
    subsequent call, satisfying (1); pytest's default collect-everything-
    then-run-everything behavior means no other test file's *collection*
    can happen after this module's *tests* have started running, so
    satisfying (2) doesn't require any teardown cleanup.
    """
    global _OPS_CONSOLE_MODULE
    if _OPS_CONSOLE_MODULE is not None:
        return _OPS_CONSOLE_MODULE
    service_path = PROJECT_ROOT / "services" / "ops-console"
    for module_name in list(sys.modules):
        if module_name == "app" or module_name.startswith("app."):
            del sys.modules[module_name]
    sys.path.insert(0, str(service_path))
    try:
        _OPS_CONSOLE_MODULE = importlib.import_module("app.main")
        return _OPS_CONSOLE_MODULE
    finally:
        try:
            sys.path.remove(str(service_path))
        except ValueError:
            pass


_OPS_CONSOLE_MODULE = None


def import_ops_console_main():
    return _import_ops_console_main_fresh()


class FakePostgres:
    """Returns deterministic fixture rows keyed by a recognizable SQL
    fragment, so ops_payload()'s dozen distinct queries each get plausible
    data without a live database.
    """

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        q = " ".join(query.split()).lower()
        if "from tenant_config" in q:
            return [{"tenant_id": "tenant_demo", "tenant_name": "Demo", "plan": "pro", "region": "us", "is_active": True}]
        if "from reconciliation_audit" in q:
            return [{"tenant_id": "tenant_demo", "metric_date": "2026-08-01", "status": "passed", "revenue_delta": 0.0, "order_count_delta": 0, "units_sold_delta": 0, "checked_at": datetime.now(timezone.utc)}]
        if "from lineage_events" in q:
            return []
        if "from event_outbox" in q:
            return [{"status": "published", "event_count": 3, "latest_update": datetime.now(timezone.utc)}]
        if "from event_inbox" in q:
            return [{"status": "processed", "event_count": 5, "latest_received": datetime.now(timezone.utc)}]
        if "from alerts" in q:
            return []
        if "from privacy_erasure_requests" in q:
            return []
        if "from pipeline_run_log" in q and "reliability:" in q:
            return [{"scenario_id": "poison-event", "status": "passed", "finished_at": datetime.now(timezone.utc)}]
        if "from pipeline_run_log" in q:
            return [{"pipeline_name": "backfill_tenant_metrics_daily", "status": "completed", "records_processed": 10, "started_at": datetime.now(timezone.utc), "finished_at": datetime.now(timezone.utc)}]
        if "from service_health_metrics" in q:
            return [{"service_name": "analytics-service", "status": "healthy", "latency_ms": 12.0, "error_count": 0, "kafka_lag": 0, "event_timestamp": datetime.now(timezone.utc)}]
        if "from stream_processing_runs" in q:
            return [{"run_id": "11111111-1111-1111-1111-111111111111", "job_name": "cloudscale-structured-streaming", "status": "running", "started_at": datetime.now(timezone.utc), "ended_at": None}]
        if "from streaming_failures" in q:
            return []
        if "from streaming_checkpoint_audit" in q:
            return [{"query_name": "aggregates", "last_commit": datetime.now(timezone.utc)}]
        if "from streaming_late_events" in q:
            return [{"classification": "late_accepted", "recent_count": 2}]
        raise AssertionError(f"FakePostgres.fetch: unrecognized query: {q[:120]}")

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        q = " ".join(query.split()).lower()
        if "tenant_metrics_daily" in q:
            return {"last_update": datetime.now(timezone.utc)}
        if "stream_window_metrics" in q:
            return {"last_update": None}
        if "reconciliation_audit" in q:
            return {"failures": 0, "total": 3}
        raise AssertionError(f"FakePostgres.fetchrow: unrecognized query: {q[:120]}")


def _module_with_fake_postgres():
    module = import_ops_console_main()
    module.postgres = FakePostgres()
    return module


def test_ops_payload_includes_streaming_sections() -> None:
    import asyncio

    module = _module_with_fake_postgres()
    payload = asyncio.run(module.ops_payload("tenant_demo"))
    assert "streaming_runs" in payload
    assert "streaming_checkpoints" in payload
    assert "streaming_failures" in payload
    assert "late_events" in payload
    assert payload["streaming_runs"][0]["job_name"] == "cloudscale-structured-streaming"
    assert payload["streaming_checkpoints"][0]["query_name"] == "aggregates"
    assert payload["late_events"][0]["classification"] == "late_accepted"


def test_ops_payload_includes_reliability_and_serving_freshness() -> None:
    import asyncio

    module = _module_with_fake_postgres()
    payload = asyncio.run(module.ops_payload("tenant_demo"))
    assert "reliability_runs" in payload
    assert payload["reliability_runs"][0]["scenario_id"] == "poison-event"
    assert payload["reliability_runs"][0]["status"] == "passed"
    assert "serving_freshness" in payload
    tables = {row["table"] for row in payload["serving_freshness"]}
    assert tables == {"tenant_metrics_daily", "stream_window_metrics"}


def test_ops_payload_includes_data_products_section() -> None:
    import asyncio

    module = _module_with_fake_postgres()
    payload = asyncio.run(module.ops_payload("tenant_demo"))
    assert "data_products" in payload
    assert len(payload["data_products"]) >= 5
    names = {p["name"] for p in payload["data_products"]}
    assert "Revenue & Order Metrics" in names
    for product in payload["data_products"]:
        assert product["tenant_scoped"] is True


def test_data_products_summary_is_resilient_to_missing_registry(monkeypatch) -> None:
    """A container image built before the registry files exist
    should degrade to an empty list, not crash the whole console page.
    """
    module = import_ops_console_main()

    def _raise():
        raise FileNotFoundError("registry.yml not found")

    monkeypatch.setattr(module, "load_data_product_registry", _raise)
    result = module._data_products_summary()
    assert result == []


def test_render_ops_produces_valid_html_with_all_sections() -> None:
    import asyncio

    module = _module_with_fake_postgres()
    payload = asyncio.run(module.ops_payload("tenant_demo"))
    html = module.render_ops(payload)
    assert html.startswith("\n<!doctype html>")
    assert "Platform Ops Console" in html
    assert "Structured Streaming" in html
    assert "Reliability" in html
    assert "Data Products" in html
    assert "Prometheus" in html
    assert "Grafana" in html
    assert "cloudscale-structured-streaming" in html
    assert "Revenue &amp; Order Metrics" in html or "Revenue & Order Metrics" in html


def test_render_ops_escapes_tenant_id_in_form() -> None:
    module = import_ops_console_main()
    html = module.render_ops(
        {
            "tenant_id": "<script>alert(1)</script>",
            "tenants": [], "reconciliation": [], "lineage": [], "outbox": [], "inbox": [],
            "incidents": [], "privacy": [], "pipelines": [], "health": [],
            "streaming_runs": [], "streaming_failures": [], "streaming_checkpoints": [],
            "reliability_runs": [], "serving_freshness": [], "late_events": [], "data_products": [],
        }
    )
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html


def test_ops_console_openapi_still_exposes_expected_routes() -> None:
    """Regression: the additions must not change the service's
    route surface (tests/test_api_contracts.py asserts this route set too).
    """
    module = import_ops_console_main()
    openapi = module.app.openapi()
    assert {"/", "/api/ops", "/health", "/metrics"}.issubset(set(openapi["paths"]))


def test_streaming_and_reliability_queries_match_observability_module() -> None:
    """The ops console must reuse app.observability's fetch_* functions,
    not a second hand-copied set of queries (the stated design —
    see app/observability.py's module docstring).
    """
    module = import_ops_console_main()
    import inspect

    source = inspect.getsource(module)
    for fn_name in (
        "fetch_checkpoint_freshness",
        "fetch_late_events_summary",
        "fetch_reliability_status",
        "fetch_serving_freshness",
    ):
        assert fn_name in source, f"ops console main.py should import/call {fn_name} from app.observability"
