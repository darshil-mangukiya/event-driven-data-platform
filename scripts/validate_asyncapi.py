"""Structurally and cross-reference validate contracts/asyncapi.yml against
the platform's real configuration — every channel must correspond to a
real Kafka topic (platform_shared.kafka.TOPIC_DEFINITIONS), every
message payload $ref must resolve to a schema file that actually exists,
and every event type listed must be a real EventType.

Usage:
    python scripts/validate_asyncapi.py
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONTRACTS_ROOT = PROJECT_ROOT / "contracts"
sys.path.insert(0, str(PROJECT_ROOT / "services" / "shared"))


def validate() -> list[str]:
    import yaml
    from platform_shared.kafka import TOPIC_DEFINITIONS
    from platform_shared.schemas import EventType

    errors: list[str] = []
    asyncapi_path = CONTRACTS_ROOT / "asyncapi.yml"
    if not asyncapi_path.exists():
        return [f"{asyncapi_path} does not exist — run scripts/generate_asyncapi.py"]

    document = yaml.safe_load(asyncapi_path.read_text())

    for key in ("asyncapi", "info", "channels", "operations", "components"):
        if key not in document:
            errors.append(f"missing top-level key: {key}")
    if errors:
        return errors

    real_event_type_values = {e.value for e in EventType}

    # Every channel must be a real, currently-configured Kafka topic.
    for topic_name, channel in document["channels"].items():
        if topic_name not in TOPIC_DEFINITIONS:
            errors.append(f"channel {topic_name!r} does not correspond to any topic in platform_shared.kafka.TOPIC_DEFINITIONS")
            continue
        for event_type in channel.get("x-event-types", []):
            if event_type not in real_event_type_values:
                errors.append(f"channel {topic_name!r} lists unknown event type: {event_type!r}")

    # Every message payload $ref must resolve to a real file.
    for message_name, message in document["components"]["messages"].items():
        payload_ref = message.get("payload", {}).get("$ref", "")
        if payload_ref.startswith("../"):
            schema_path = CONTRACTS_ROOT / payload_ref[3:]
            if not schema_path.exists():
                errors.append(f"message {message_name!r} payload $ref does not resolve to a real file: {payload_ref}")

    # Every operation's channel $ref must resolve to a declared channel.
    for op_id, operation in document["operations"].items():
        channel_ref = operation.get("channel", {}).get("$ref", "")
        referenced_channel = channel_ref.replace("#/channels/", "")
        if referenced_channel not in document["channels"]:
            errors.append(f"operation {op_id!r} references undeclared channel: {channel_ref}")

    # Every real topic in TOPIC_DEFINITIONS should have a channel entry —
    # catches the AsyncAPI doc silently falling behind a new topic added
    # to the real Kafka config.
    for real_topic_name in TOPIC_DEFINITIONS:
        if real_topic_name not in document["channels"]:
            errors.append(f"real Kafka topic {real_topic_name!r} has no corresponding AsyncAPI channel")

    return errors


def main() -> None:
    errors = validate()
    if errors:
        print("\n".join(errors), file=sys.stderr)
        raise SystemExit(1)
    print("asyncapi spec cross-references real topics/schemas/event-types ok")


if __name__ == "__main__":
    main()
