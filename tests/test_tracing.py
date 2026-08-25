"""Tests for the corresponding verification's OpenTelemetry distributed tracing
(services/shared/platform_shared/tracing.py).

The full live trace (ingestion-service HTTP request -> kafka.publish ->
processing-service kafka.consume, one continuous trace across the real
Kafka boundary) was verified against a real local Jaeger instance during
development — see evidence/validation/opentelemetry-verification.md for
the full trace JSON. These tests cover the disabled-by-default contract
(the platform must run identically with zero tracing dependencies when
OTEL_ENABLED is unset) and the propagation helpers' no-op/enabled
behavior in isolation.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "services" / "shared"))

from platform_shared.tracing import (  # noqa: E402
    extract_trace_context_from_headers,
    inject_trace_context_into_headers,
    traced_span,
    tracing_enabled,
)


def test_tracing_disabled_by_default() -> None:
    with patch.dict("os.environ", {}, clear=False):
        import os

        os.environ.pop("OTEL_ENABLED", None)
        assert tracing_enabled() is False


def test_tracing_enabled_when_env_var_set() -> None:
    with patch.dict("os.environ", {"OTEL_ENABLED": "true"}):
        assert tracing_enabled() is True


def test_inject_trace_context_is_a_noop_when_disabled() -> None:
    with patch.dict("os.environ", {"OTEL_ENABLED": "false"}):
        headers = inject_trace_context_into_headers({"existing": "header"})
    assert headers == {"existing": "header"}


def test_extract_trace_context_returns_none_when_disabled() -> None:
    with patch.dict("os.environ", {"OTEL_ENABLED": "false"}):
        result = extract_trace_context_from_headers({"traceparent": "00-abc-def-01"})
    assert result is None


def test_traced_span_yields_none_and_does_not_raise_when_disabled() -> None:
    with patch.dict("os.environ", {"OTEL_ENABLED": "false"}):
        with traced_span("test-service", "test-span") as span:
            assert span is None
        # No exception, no real span machinery touched — must work with
        # zero OpenTelemetry configuration.


def test_traced_span_creates_a_real_span_when_enabled() -> None:
    with patch.dict("os.environ", {"OTEL_ENABLED": "true"}):
        with traced_span("test-service", "test-span", {"key": "value"}) as span:
            assert span is not None
            assert span.is_recording()


def test_inject_then_extract_round_trips_a_real_trace_context() -> None:
    """Real, non-mocked OpenTelemetry propagation round-trip: a span
    started here, injected into a headers dict, extracted back out, and
    activated — proves the actual W3C traceparent propagation mechanism
    the ingestion-service -> Kafka -> processing-service path uses (the
    live Jaeger verification exercises this exact mechanism across a real
    Kafka message, beyond in-process).
    """
    with patch.dict("os.environ", {"OTEL_ENABLED": "true"}):
        with traced_span("producer-service", "producer-span") as producer_span:
            producer_trace_id = producer_span.get_span_context().trace_id
            headers = inject_trace_context_into_headers({})

        assert "traceparent" in headers

        context = extract_trace_context_from_headers(headers)
        with traced_span("consumer-service", "consumer-span", parent_context=context) as consumer_span:
            consumer_trace_id = consumer_span.get_span_context().trace_id

        assert consumer_trace_id == producer_trace_id, "consumer span must belong to the same trace as the producer span"
