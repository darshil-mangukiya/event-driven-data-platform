"""Tests for scripts/dlq_tool.py::extract_dlq_record — the pure parsing
logic that turns a raw DLQ Kafka message into a structured DlqRecord.

coverage analysis found scripts/dlq_tool.py at 0% coverage. Most of
the file needs a live Kafka broker (read_dlq_records, replay) or a live
database (audit_replay) and legitimately can't be unit tested without one
— but extract_dlq_record() is pure string/JSON parsing with no I/O at all.

Fixtures here mirror the *actual* DLQ envelope shape written by
platform_shared.kafka.KafkaEventProducer.publish_dlq — event_type is
rewritten to system.alert, and payload.message carries a JSON string with
failed_stage/error/original_event, exactly like the real producer builds
it (not a shape invented for this test).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from platform_shared.schemas import EventType, build_envelope, envelope_to_json

from scripts.dlq_tool import extract_dlq_record

_ORIGINAL_ORDER_PAYLOAD = {
    "order_id": "ord_1",
    "customer_id": "cust_1",
    "product_id": "prod_1",
    "quantity": 1,
    "unit_price": 10.0,
}


def _original_envelope_dict() -> dict:
    """The event that originally failed — dumped the same way
    publish_dlq() does it: envelope.model_dump(mode="json").
    """
    envelope = build_envelope(
        tenant_id="tenant_demo",
        event_type=EventType.ORDER_CREATED,
        source_service="ingestion-service",
        payload=_ORIGINAL_ORDER_PAYLOAD,
        event_timestamp=datetime(2026, 8, 1, tzinfo=timezone.utc),
    )
    return json.loads(envelope.model_dump_json())


def _dlq_envelope_json(*, message: str | None, extra_payload: dict | None = None) -> str:
    """Mirrors platform_shared.kafka.KafkaEventProducer.publish_dlq: the
    DLQ envelope's own event_type is system.alert, and its payload is a
    SystemPayload (service_name/status/error_count/message).
    """
    payload = {
        "service_name": "validation",
        "status": "dlq",
        "error_count": 1,
        **(extra_payload or {}),
    }
    if message is not None:
        payload["message"] = message
    envelope = build_envelope(
        tenant_id="tenant_demo",
        event_type=EventType.SYSTEM_ALERT,
        source_service="validation",
        payload=payload,
        event_timestamp=datetime(2026, 8, 1, tzinfo=timezone.utc),
    )
    return envelope_to_json(envelope)


def test_extract_dlq_record_parses_real_publish_dlq_shape() -> None:
    """The exact shape platform_shared.kafka.KafkaEventProducer.publish_dlq
    produces: payload.message is a JSON string of
    {failed_stage, error, original_event}.
    """
    dlq_payload = {
        "failed_stage": "validation",
        "error": "unsupported_payload_version",
        "original_event": _original_envelope_dict(),
    }
    raw = _dlq_envelope_json(message=json.dumps(dlq_payload))

    record = extract_dlq_record(raw, topic="platform.events.dlq", partition=0, offset=42)

    assert record.error == "unsupported_payload_version"
    assert record.failed_stage == "validation"
    assert record.original_event is not None
    assert record.original_event.payload["order_id"] == "ord_1"
    assert record.topic == "platform.events.dlq"
    assert record.partition == 0
    assert record.offset == 42


def test_extract_dlq_record_handles_non_json_message_as_plain_error_string() -> None:
    """message isn't guaranteed to be structured JSON — a plain string must
    become the error text, not raise or silently drop the record.
    """
    raw = _dlq_envelope_json(message="connection refused")

    record = extract_dlq_record(raw, topic="platform.events.dlq", partition=0, offset=1)

    assert record.error == "connection refused"
    assert record.failed_stage is None
    assert record.original_event is None


def test_extract_dlq_record_falls_back_to_payload_level_original_event() -> None:
    """Some producers could put original_event directly under payload
    rather than nested inside a message JSON string.

    ``extract_dlq_record`` has a fallback branch for exactly this shape
    (``envelope.payload.get("original_event")``) — but this test proves
    that branch is **currently unreachable dead code**: the DLQ envelope's
    own payload is always validated against ``SystemPayload`` (the schema
    for ``event_type=system.alert``, which is what
    ``platform_shared.kafka.KafkaEventProducer.publish_dlq`` always uses),
    and ``SystemPayload`` has no ``original_event`` field — so
    ``EventEnvelope``'s ``validate_payload_contract`` model validator
    silently strips it on *every* construction path, including
    ``envelope_from_json`` on a raw, hand-built JSON string that never
    went through ``build_envelope`` at all. A real, if minor, finding from
    coverage analysis: the fallback branch in
    ``scripts/dlq_tool.py::extract_dlq_record`` cannot fire against any
    envelope this platform's own schema layer would ever produce or parse
    — documented here rather than pretended to work.
    """
    raw = _dlq_envelope_json(message=None, extra_payload={"original_event": _original_envelope_dict()})

    record = extract_dlq_record(raw, topic="platform.events.dlq", partition=1, offset=7)

    assert record.original_event is None


def test_extract_dlq_record_with_no_message_and_no_original_event() -> None:
    """A DLQ envelope with neither field must still produce a valid
    record — no error, no original_event — rather than raising.
    """
    raw = _dlq_envelope_json(message=None)

    record = extract_dlq_record(raw, topic="platform.events.dlq", partition=0, offset=0)

    assert record.error is None
    assert record.failed_stage is None
    assert record.original_event is None
    assert record.envelope.payload["status"] == "dlq"


def test_extract_dlq_record_preserves_the_dlq_envelope_itself() -> None:
    """record.envelope must always be the DLQ message's own envelope
    (distinct from record.original_event, the event that originally
    failed) — replay logic depends on this distinction.
    """
    raw = _dlq_envelope_json(message="boom")
    record = extract_dlq_record(raw, topic="platform.events.dlq", partition=2, offset=99)

    assert record.envelope.tenant_id == "tenant_demo"
    assert record.envelope.event_type == EventType.SYSTEM_ALERT
    assert record.original_event is None
    assert record.error == "boom"


def test_extract_dlq_record_normalizes_spark_streaming_dlq_shape() -> None:
    """Spark writes the rejected raw envelope beside its rejection reason."""
    original = _original_envelope_dict()
    raw = json.dumps(
        {
            "raw_value": json.dumps(original),
            "rejection_reason": "event_time_watermark_exceeded",
        }
    )

    record = extract_dlq_record(raw, topic="platform.events.dlq", partition=0, offset=30)

    assert record.envelope.event_id == original["event_id"]
    assert record.original_event is not None
    assert record.original_event.event_id == original["event_id"]
    assert record.error == "event_time_watermark_exceeded"
    assert record.failed_stage == "spark-streaming"
