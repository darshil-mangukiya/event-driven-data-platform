"""Sink retry/failure-handling behavior, verified with a mocked PostgreSQL connection.

Real end-to-end writes against a live PostgreSQL are covered by the
``reliability/`` db-outage exercise and by ``make streaming-demo`` against
the dockerized stack, not by this fast unit suite (see
docs/streaming_architecture.md, "Runtime Verification").
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from spark.streaming.config import StreamingConfig
from spark.streaming.sinks import PostgresSink, SinkError, _with_retries


@pytest.fixture
def config():
    return StreamingConfig(sink_max_retries=3, sink_retry_backoff_seconds=0.0)


def test_with_retries_succeeds_first_try(config):
    calls = []
    _with_retries(config, "test-sink", lambda: calls.append(1))
    assert calls == [1]


def test_with_retries_retries_then_succeeds(config):
    attempts = {"count": 0}

    def flaky():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ConnectionError("db unavailable")

    _with_retries(config, "test-sink", flaky)
    assert attempts["count"] == 3


def test_with_retries_raises_sink_error_after_exhausting_attempts(config):
    def always_fails():
        raise ConnectionError("db unavailable")

    with pytest.raises(SinkError):
        _with_retries(config, "test-sink", always_fails)


def test_write_window_metrics_is_noop_for_empty_batch(config):
    sink = PostgresSink(config)
    empty_df = MagicMock()
    empty_df.collect.return_value = []
    with patch("spark.streaming.sinks._connect") as mock_connect:
        written = sink.write_window_metrics(empty_df, batch_id=1)
    assert written == 0
    mock_connect.assert_not_called()


def test_write_window_metrics_issues_upsert_with_on_conflict(config):
    sink = PostgresSink(config)
    row = {
        "tenant_id": "tenant_demo",
        "window_start": "2026-08-18T00:00:00",
        "window_end": "2026-08-18T00:05:00",
        "event_domain": "orders",
        "metric_name": "revenue",
        "metric_value": 100.0,
        "event_count": 2,
    }
    df = MagicMock()
    df.collect.return_value = [row]

    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("spark.streaming.sinks._connect", return_value=mock_conn), patch(
        "psycopg2.extras.execute_values"
    ) as mock_execute_values:
        written = sink.write_window_metrics(df, batch_id=7)

    assert written == 1
    assert mock_execute_values.called
    sql_text = mock_execute_values.call_args[0][1]
    assert "on conflict" in sql_text.lower()
    assert "stream_window_metrics" in sql_text


def test_log_failure_never_raises_even_if_db_unreachable(config):
    sink = PostgresSink(config)
    with patch("spark.streaming.sinks._connect", side_effect=ConnectionError("db down")):
        # Must not raise — failure logging is best-effort.
        sink.log_failure(batch_id=1, tenant_id="tenant_demo", stage="test", error_message="boom")


def test_write_lineage_event_never_raises_even_if_db_unreachable(config):
    sink = PostgresSink(config)
    with patch("spark.streaming.sinks._connect", side_effect=ConnectionError("db down")):
        # Must not raise — lineage logging is best-effort, same principle
        # as log_failure above.
        sink._write_lineage_event(job_name="cloudscale-structured-streaming", status="running")


def test_write_lineage_event_inserts_correlated_run_id(config):
    sink = PostgresSink(config, run_id="11111111-1111-1111-1111-111111111111")

    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("spark.streaming.sinks._connect", return_value=mock_conn):
        sink._write_lineage_event(job_name="cloudscale-structured-streaming", status="running")

    assert mock_cursor.execute.called
    sql_text, params = mock_cursor.execute.call_args[0]
    assert "lineage_events" in sql_text
    assert params[2] == "11111111-1111-1111-1111-111111111111"  # run_id, same as stream_processing_runs


def test_start_run_and_finish_run_write_a_lineage_event(config):
    sink = PostgresSink(config)

    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("spark.streaming.sinks._connect", return_value=mock_conn):
        sink.start_run("cloudscale-structured-streaming", {"app_name": "test"})
        sink.finish_run("completed")

    # start_run does 1 stream_processing_runs insert + 1 lineage_events insert;
    # finish_run does 1 update + 1 lineage_events insert = 4 total.
    all_sql = [call.args[0] for call in mock_cursor.execute.call_args_list]
    assert sum("lineage_events" in sql for sql in all_sql) == 2
    assert sum("stream_processing_runs" in sql for sql in all_sql) == 2
