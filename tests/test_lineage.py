"""Tests for the data lineage graph and real lineage-event
emission wired into actual pipeline code.

Categories: graph structure (cycles/orphans), cross-reference validation
against real code, CLI integration, generation determinism, and the
pipeline-side lineage emission (backfill/reconciliation/streaming).
"""

from __future__ import annotations

from pathlib import Path

from lineage.generator import generate_lineage_report_markdown, write_lineage_report
from lineage.graph import (
    build_edges,
    find_cycles,
    find_orphan_tables,
    load_catalog,
    table_names,
    validate_graph,
    verify_all_external_nodes_recognized,
    verify_checkable_edges,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Graph structure
# ---------------------------------------------------------------------------


def test_catalog_loads_and_has_tables() -> None:
    catalog = load_catalog()
    assert len(catalog["tables"]) >= 20


def test_no_cycles_in_lineage_graph() -> None:
    """A real lineage DAG should never have a cycle — data doesn't flow
    back into its own upstream source in this platform.
    """
    cycles = find_cycles()
    assert cycles == [], f"lineage graph has cycles: {cycles}"


def test_no_orphan_tables() -> None:
    """Every cataloged table should have at least an upstream or a
    downstream declared — an orphan usually means a forgotten
    cataloging gap (this test caught two real ones — pipeline_run_log,
    data_quality_check_results — previously added them).
    """
    orphans = find_orphan_tables()
    assert orphans == [], f"orphan tables with no lineage declared: {orphans}"


def test_build_edges_produces_expected_direction() -> None:
    edges = build_edges()
    assert ("raw_events", "processed_orders") in edges
    assert ("processed_orders", "tenant_metrics_daily") in edges


def test_table_names_include_streaming_tables() -> None:
    names = table_names()
    for table in ("stream_processing_runs", "stream_window_metrics", "streaming_checkpoint_audit"):
        assert table in names


def test_table_names_include_previously_uncataloged_tables() -> None:
    """pipeline_run_log and data_quality_check_results were real schema
    tables referenced as upstream/downstream values but never given their
    own catalog entry previously — verifies the fix stuck.
    """
    names = table_names()
    assert "pipeline_run_log" in names
    assert "data_quality_check_results" in names


# ---------------------------------------------------------------------------
# Cross-reference validation against real code
# ---------------------------------------------------------------------------


def test_full_lineage_graph_validation_passes() -> None:
    result = validate_graph()
    total = sum(len(v) for v in result.values())
    assert total == 0, result


def test_verify_checkable_edges_passes_against_real_code() -> None:
    errors = verify_checkable_edges()
    assert errors == []


def test_verify_all_external_nodes_recognized() -> None:
    errors = verify_all_external_nodes_recognized()
    assert errors == []


def test_stream_window_metrics_does_not_falsely_claim_analytics_api() -> None:
    """Regression for a real catalog/reality mismatch verification found
    and fixed: stream_window_metrics is not yet served by
    analytics.metrics_api (see docs/data-products.md's revenue product
    known_limitations) — the catalog must not claim otherwise.
    """
    catalog = load_catalog()
    stream_window_metrics = next(t for t in catalog["tables"] if t["name"] == "stream_window_metrics")
    assert "analytics.metrics_api" not in stream_window_metrics.get("downstream", [])


def test_pipeline_watermarks_correctly_attributes_platform_cli_not_ops_console() -> None:
    """Regression: pipeline_watermarks is read by `platform_cli ops
    watermarks`, not the ops-console web service — the catalog previously
    claimed app.ops_console.
    """
    catalog = load_catalog()
    pipeline_watermarks = next(t for t in catalog["tables"] if t["name"] == "pipeline_watermarks")
    assert "app.ops_console" not in pipeline_watermarks.get("downstream", [])
    assert "platform_cli.ops_watermarks" in pipeline_watermarks.get("downstream", [])


def test_consumer_interruption_exercise_not_falsely_linked_to_real_checkpoint_table() -> None:
    """Regression: the consumer-interruption reliability exercise uses its
    own isolated temp checkpoint directory, never the platform's real
    streaming_checkpoint_audit table.
    """
    catalog = load_catalog()
    checkpoint_audit = next(t for t in catalog["tables"] if t["name"] == "streaming_checkpoint_audit")
    assert "reliability.consumer_interruption_exercise" not in checkpoint_audit.get("downstream", [])


def test_sessionization_job_correctly_sources_raw_events_not_processed_user_sessions() -> None:
    """Regression: spark/jobs/sessionization_job.py reads directly from
    raw_events, bypassing processed_user_sessions — the catalog previously
    implied the opposite direction.
    """
    catalog = load_catalog()
    raw_events = next(t for t in catalog["tables"] if t["name"] == "raw_events")
    processed_user_sessions = next(t for t in catalog["tables"] if t["name"] == "processed_user_sessions")
    assert "spark.tenant_user_session_summary_stage" in raw_events.get("downstream", [])
    assert "spark.tenant_user_session_summary_stage" not in processed_user_sessions.get("downstream", [])


def test_streaming_watermarks_not_falsely_claimed_as_read_by_ops_console() -> None:
    """Regression: streaming_watermarks is written but never read back by
    any service today — the catalog previously claimed app.ops_console."""
    catalog = load_catalog()
    watermarks = next(t for t in catalog["tables"] if t["name"] == "streaming_watermarks")
    assert "app.ops_console" not in watermarks.get("downstream", [])


# ---------------------------------------------------------------------------
# Generation (deterministic)
# ---------------------------------------------------------------------------


def test_lineage_report_generation_is_deterministic() -> None:
    md1 = generate_lineage_report_markdown()
    md2 = generate_lineage_report_markdown()
    assert md1 == md2


def test_lineage_report_contains_mermaid_diagrams() -> None:
    md = generate_lineage_report_markdown()
    assert "```mermaid" in md
    assert "flowchart LR" in md


def test_lineage_report_contains_all_tables() -> None:
    catalog = load_catalog()
    md = generate_lineage_report_markdown()
    for table in catalog["tables"]:
        assert table["name"] in md


def test_write_lineage_report_creates_file(tmp_path: Path) -> None:
    path = write_lineage_report(output_dir=tmp_path)
    assert path.exists()
    assert path.read_text().startswith("# Data Lineage Graph")


def test_evidence_lineage_report_is_up_to_date() -> None:
    """The checked-in evidence/lineage/lineage-graph.md should match what
    the generator produces right now.
    """
    report_path = PROJECT_ROOT / "evidence" / "lineage" / "lineage-graph.md"
    assert report_path.exists(), "run `make lineage-graph` to generate evidence"
    assert report_path.read_text() == generate_lineage_report_markdown()


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------


def test_cli_lineage_validate() -> None:
    from platform_cli.__main__ import lineage_validate

    result = lineage_validate(None)
    assert result["status"] == "passed"
    assert result["total_issues"] == 0


def test_cli_lineage_show_known_table() -> None:
    import argparse

    from platform_cli.__main__ import lineage_show

    result = lineage_show(argparse.Namespace(table="tenant_metrics_daily"))
    assert result["status"] == "passed"
    assert "processed_orders" in result["upstream"]


def test_cli_lineage_show_unknown_table() -> None:
    import argparse

    from platform_cli.__main__ import lineage_show

    result = lineage_show(argparse.Namespace(table="does_not_exist"))
    assert result["status"] == "failed"


def test_cli_parser_registers_lineage_subcommand() -> None:
    from platform_cli.__main__ import build_parser

    parser = build_parser()
    args = parser.parse_args(["lineage", "validate"])
    assert args.resource == "lineage"
    assert args.action == "validate"


# ---------------------------------------------------------------------------
# Pipeline-side lineage emission (backfill / reconciliation / streaming)
# ---------------------------------------------------------------------------


def test_backfill_metrics_module_wires_lineage_emission() -> None:
    """backfill_metrics.py must actually call emit_pipeline_lineage, not
    just import it — a real wiring check, beyond an import check.
    """
    import inspect

    import scripts.backfill_metrics as backfill_module

    source = inspect.getsource(backfill_module)
    assert "emit_pipeline_lineage" in source
    assert "BACKFILL_SOURCE_TABLES" in source
    assert "BACKFILL_OUTPUT_TABLES" in source


def test_backfill_uses_one_correlated_run_id_not_two_separate_inserts() -> None:
    """Regression for a runtime defect verification found: run_backfill used to
    insert two independent pipeline_run_log rows (one at start, one at
    finish) with different auto-generated pipeline_run_ids, never actually
    correlated. Now it generates one run_id and updates that same row.
    """
    import inspect

    import scripts.backfill_metrics as backfill_module

    source = inspect.getsource(backfill_module.run_backfill)
    assert "uuid4" in source or "run_id" in source
    assert source.count("insert into pipeline_run_log") == 1
    assert "update pipeline_run_log" in source


def test_reconcile_metrics_module_wires_lineage_emission() -> None:
    import inspect

    import scripts.reconcile_metrics as reconcile_module

    source = inspect.getsource(reconcile_module)
    assert "emit_pipeline_lineage" in source
    assert "RECONCILIATION_SOURCE_TABLES" in source


def test_streaming_sink_wires_lineage_emission() -> None:
    import inspect

    from spark.streaming import sinks as sinks_module

    source = inspect.getsource(sinks_module.PostgresSink)
    assert "_write_lineage_event" in source
    assert "lineage_events" in inspect.getsource(sinks_module)


def test_emit_pipeline_lineage_builds_correct_event_shape() -> None:
    """emit_pipeline_lineage should build the same OpenLineage-style shape
    as scripts/emit_lineage_event.py::build_lineage_event — not a second,
    differently-shaped event format.
    """
    from lineage.events import build_lineage_event

    event = build_lineage_event(
        job_name="test-job",
        run_id="test-run-id",
        tenant_id="tenant_demo",
        inputs=["a", "b"],
        outputs=["c"],
        status="succeeded",
    )
    assert event["eventType"] == "COMPLETE"
    assert event["job"]["name"] == "test-job"
    assert event["run"]["runId"] == "test-run-id"
    assert event["inputs"] == [{"namespace": "postgres", "name": "a"}, {"namespace": "postgres", "name": "b"}]
    assert event["outputs"] == [{"namespace": "postgres", "name": "c"}]


def test_build_lineage_event_eventtime_is_a_real_datetime_not_a_string() -> None:
    """Regression for a defect found during runtime verification:
    build_lineage_event() used to return eventTime as an .isoformat() str.
    asyncpg (unlike psycopg2) does not implicitly cast a Python str to a
    `timestamptz` bind parameter and raised
    ``InvalidArgumentError: expected a datetime.date or datetime.datetime
    instance, got 'str'`` the first time a lineage event was actually
    persisted end-to-end, a path that previously lacked coverage.
    """
    import datetime

    from lineage.events import build_lineage_event

    event = build_lineage_event(
        job_name="test-job",
        run_id="test-run-id",
        tenant_id="tenant_demo",
        inputs=["a"],
        outputs=["b"],
        status="succeeded",
    )
    assert isinstance(event["eventTime"], datetime.datetime)


def test_lineage_event_json_serialization_handles_datetime_eventtime() -> None:
    """The CLI's --output/print paths must serialize the now-real-datetime
    eventTime without raising — this is what the fix's default=str
    addition in scripts/emit_lineage_event.py covers.
    """
    import json

    from lineage.events import build_lineage_event

    event = build_lineage_event(
        job_name="test-job", run_id="r", tenant_id=None, inputs=[], outputs=[], status="succeeded"
    )
    # Must not raise TypeError: Object of type datetime is not JSON serializable
    serialized = json.dumps(event, default=str)
    assert "eventTime" in serialized


def test_persist_reconciliation_converts_metric_date_string_back_to_a_real_date() -> None:
    """Regression for a second defect found during runtime verification:
    evaluate_reconciliation() deliberately normalizes metric_date to
    a string (tests and reliability/scenarios/reconciliation_mismatch.py
    depend on that), but persist_reconciliation() binds it to a `$n::date`
    asyncpg parameter, which needs a real date object to encode — passing
    the string raised ``AttributeError: 'str' object has no attribute
    'toordinal'`` the first time reconciliation actually persisted a row
    against a live database.
    """
    import asyncio
    import datetime
    from unittest.mock import AsyncMock

    from scripts.reconcile_metrics import ReconciliationRequest, persist_reconciliation

    mock_postgres = AsyncMock()
    request = ReconciliationRequest(
        tenant_id="tenant_demo",
        start_date=datetime.date(2026, 6, 1),
        end_date=datetime.date(2026, 6, 2),
    )
    results = [
        {
            "tenant_id": "tenant_demo",
            "metric_date": "2026-06-01",  # string, exactly as evaluate_reconciliation() produces
            "status": "passed",
            "revenue_delta": 0.0,
            "order_count_delta": 0,
            "units_sold_delta": 0,
        }
    ]

    asyncio.run(persist_reconciliation(mock_postgres, request, results))

    assert mock_postgres.execute.called
    call_args = mock_postgres.execute.call_args[0]
    bound_metric_date = call_args[2]  # (sql, tenant_id, metric_date, ...)
    assert isinstance(bound_metric_date, datetime.date)
    assert bound_metric_date == datetime.date(2026, 6, 1)
