"""Tests for reliability/injectors/reachability.py.

This module had no dedicated test file previously.
`postgres_reachable()` had a bug (passing a float `connect_timeout` to
psycopg2, which only accepts whole seconds) that made it silently return
False *even when PostgreSQL was reachable*, for every call using
the default timeout. The broad `except Exception: return False` meant
the issue surfaced during live local-initialization
verification, which noticed `postgres_reachable()` disagreed with a direct
`psycopg2.connect()` call against the same live database.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from reliability.injectors.reachability import (
    kafka_reachable,
    postgres_reachable,
    redis_reachable,
    tcp_reachable,
)


def test_postgres_reachable_passes_an_integer_connect_timeout_to_psycopg2() -> None:
    """Regression: psycopg2's connect_timeout option only accepts whole
    seconds — passing the float default (2.0) raised
    `OperationalError: invalid integer value "2.0" for connection option
    "connect_timeout"`, silently caught and turned into a wrong `False`.
    """
    mock_connect = MagicMock()
    with patch("psycopg2.connect", mock_connect):
        postgres_reachable("postgresql://platform:platform@localhost:5432/data_platform")

    assert mock_connect.called
    _args, kwargs = mock_connect.call_args
    assert isinstance(kwargs["connect_timeout"], int), (
        f"connect_timeout must be an int, got {type(kwargs['connect_timeout'])}: {kwargs['connect_timeout']!r}"
    )


def test_postgres_reachable_rounds_up_sub_second_timeouts_not_down_to_zero() -> None:
    """libpq treats connect_timeout=0 as "no timeout" — truncating a
    sub-second float (e.g. 0.5) down to 0 would silently disable the
    timeout entirely, the opposite of what a caller passing a short
    timeout intends. Must round up to at least 1.
    """
    mock_connect = MagicMock()
    with patch("psycopg2.connect", mock_connect):
        postgres_reachable("postgresql://platform:platform@localhost:5432/data_platform", timeout=0.5)

    _args, kwargs = mock_connect.call_args
    assert kwargs["connect_timeout"] == 1


def test_postgres_reachable_returns_true_when_connect_succeeds() -> None:
    mock_conn = MagicMock()
    with patch("psycopg2.connect", return_value=mock_conn):
        assert postgres_reachable("postgresql://platform:platform@localhost:5432/data_platform") is True
    assert mock_conn.close.called


def test_postgres_reachable_returns_false_when_connect_raises() -> None:
    with patch("psycopg2.connect", side_effect=ConnectionError("refused")):
        assert postgres_reachable("postgresql://platform:platform@192.0.2.1:5432/data_platform") is False


def test_tcp_reachable_returns_false_for_a_closed_port() -> None:
    # RFC 5737 TEST-NET-1 — reserved, never routable, used elsewhere in
    # this repo's reliability scenarios for the same "deliberately
    # unreachable" purpose.
    assert tcp_reachable("192.0.2.1", 5432, timeout=0.5) is False


def test_redis_reachable_parses_host_and_port_from_url() -> None:
    with patch("reliability.injectors.reachability.tcp_reachable", return_value=True) as mock_tcp:
        result = redis_reachable("redis://myhost:6380/0")
    assert result is True
    mock_tcp.assert_called_once_with("myhost", 6380, timeout=1.5)


def test_redis_reachable_defaults_to_port_6379() -> None:
    with patch("reliability.injectors.reachability.tcp_reachable", return_value=True) as mock_tcp:
        redis_reachable("redis://myhost/0")
    mock_tcp.assert_called_once_with("myhost", 6379, timeout=1.5)


def test_kafka_reachable_parses_first_bootstrap_server() -> None:
    with patch("reliability.injectors.reachability.tcp_reachable", return_value=True) as mock_tcp:
        result = kafka_reachable("kafka1:9092,kafka2:9092")
    assert result is True
    mock_tcp.assert_called_once_with("kafka1", 9092, timeout=2.0)


def test_kafka_reachable_returns_false_for_malformed_bootstrap_string() -> None:
    assert kafka_reachable("not-a-host-port") is False
    assert kafka_reachable("host:not-a-port") is False
