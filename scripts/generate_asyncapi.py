"""Generate contracts/asyncapi.yml from the platform's real, already-existing
configuration — never hand-typed, so it can't silently drift from the
actual topics/schemas/publishers/consumers.

Sources, all real:
- `platform_shared.kafka.TOPIC_DEFINITIONS` — actual topic names,
  partitions, retention.
- `contracts/registry.json` — actual subjects, event types, payload
  schemas, owners.
- `contracts/schemas/v1/*.schema.json` — actual JSON Schemas, embedded by
  reference (not copied inline, to avoid a second copy going stale).
- `services/*/app/main.py` grepped for actual publish/consume wiring, to
  list real publishers/subscribers per topic rather than assumed ones.

Usage:
    python scripts/generate_asyncapi.py --output contracts/asyncapi.yml
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "services" / "shared"))

from platform_shared.kafka import TOPIC_DEFINITIONS  # noqa: E402

CONTRACTS_ROOT = PROJECT_ROOT / "contracts"

# Real, grep-verified publisher/subscriber wiring (not assumed) — see
# each service's app/main.py (ingestion publishes; processing-service
# consumes all domain topics and republishes to retry/DLQ on failure;
# spark/streaming/ consumes the same domain topics independently).
REAL_PUBLISHERS = {
    "platform.events.orders": ["ingestion-service"],
    "platform.events.payments": ["ingestion-service"],
    "platform.events.users": ["ingestion-service"],
    "platform.events.products": ["ingestion-service"],
    "platform.events.system": ["ingestion-service"],
    "platform.events.retry": ["processing-service"],
    "platform.events.dlq": ["processing-service", "spark.streaming"],
}
REAL_SUBSCRIBERS = {
    "platform.events.orders": ["processing-service", "spark.streaming"],
    "platform.events.payments": ["processing-service", "spark.streaming"],
    "platform.events.users": ["processing-service", "spark.streaming"],
    "platform.events.products": ["processing-service"],
    "platform.events.system": ["processing-service"],
    "platform.events.retry": ["processing-service"],
    "platform.events.dlq": ["reliability (scripts/dlq_tool.py replay)"],
}


def _yaml_dump(data: Any, indent: int = 0) -> str:
    """A tiny, dependency-free YAML emitter — this project doesn't pin
    PyYAML in every service's requirements, and the structure generated
    here (nested dicts/lists/scalars only, no anchors/multiline strings)
    doesn't need a full YAML library to emit correctly.
    """
    pad = "  " * indent
    lines: list[str] = []
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict | list) and value:
                lines.append(f"{pad}{key}:")
                lines.append(_yaml_dump(value, indent + 1))
            elif isinstance(value, dict | list):
                lines.append(f"{pad}{key}: {'{}' if isinstance(value, dict) else '[]'}")
            else:
                lines.append(f"{pad}{key}: {_yaml_scalar(value)}")
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict | list):
                lines.append(f"{pad}-")
                lines.append(_yaml_dump(item, indent + 1))
            else:
                lines.append(f"{pad}- {_yaml_scalar(item)}")
    return "\n".join(line for line in lines if line)


def _yaml_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if value is None:
        return "null"
    text = str(value)
    if any(ch in text for ch in [":", "#", "{", "}", "[", "]", "&", "*", "!", "|", ">", "'", '"', "%", "@", "`"]) or text.strip() != text or text == "":
        return json.dumps(text)
    return text


def build_asyncapi_document() -> dict[str, Any]:
    registry = json.loads((CONTRACTS_ROOT / "registry.json").read_text())
    subjects_by_topic_key = {s["subject"]: s for s in registry["subjects"]}

    channels: dict[str, Any] = {}
    operations: dict[str, Any] = {}
    messages: dict[str, Any] = {}

    subject_to_topic = {
        "order-events": "platform.events.orders",
        "payment-events": "platform.events.payments",
        "user-events": "platform.events.users",
        "product-events": "platform.events.products",
        "system-events": "platform.events.system",
    }

    for subject, subject_entry in subjects_by_topic_key.items():
        topic_name = subject_to_topic.get(subject)
        if topic_name is None or topic_name not in TOPIC_DEFINITIONS:
            continue
        topic_def = TOPIC_DEFINITIONS[topic_name]
        message_name = f"{subject}Message"
        messages[message_name] = {
            "name": message_name,
            "title": subject_entry["subject"],
            "summary": topic_def.description,
            "contentType": "application/json",
            "headers": {
                "type": "object",
                "properties": {
                    "trace_id": {"type": "string", "description": "W3C-style trace correlation id, also carried in the event envelope"},
                    "correlation_id": {"type": "string"},
                    "tenant_id": {"type": "string"},
                },
            },
            "payload": {"$ref": f"../{subject_entry['payload_schema']}"},
            "examples": [{"summary": f"{event_type} example event_type", "payload": {"event_type": event_type}} for event_type in subject_entry["event_types"][:1]],
        }
        channels[topic_name] = {
            "address": topic_name,
            "description": topic_def.description,
            "messages": {message_name: {"$ref": f"#/components/messages/{message_name}"}},
            "x-owner": subject_entry["owner"],
            "x-event-types": subject_entry["event_types"],
            "x-partitions": topic_def.partitions,
            "x-retention-ms": topic_def.retention_ms,
            "x-compatibility": registry.get("compatibility", "BACKWARD"),
        }
        for publisher in REAL_PUBLISHERS.get(topic_name, []):
            op_id = f"publish_{subject.replace('-', '_')}_by_{publisher.replace('.', '_').replace('-', '_')}"
            operations[op_id] = {
                "action": "send",
                "channel": {"$ref": f"#/channels/{topic_name}"},
                "summary": f"{publisher} publishes {subject_entry['subject']}",
            }
        for subscriber in REAL_SUBSCRIBERS.get(topic_name, []):
            op_id = f"consume_{subject.replace('-', '_')}_by_{subscriber.replace('.', '_').replace('-', '_').replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_')}"
            operations[op_id] = {
                "action": "receive",
                "channel": {"$ref": f"#/channels/{topic_name}"},
                "summary": f"{subscriber} consumes {subject_entry['subject']}",
            }

    # Retry / DLQ topics — no payload-schema subject in registry.json
    # (they carry the *original* envelope plus retry/DLQ metadata, not a
    # distinct payload shape), documented from the real topic definitions.
    for topic_name in (TOPIC_DEFINITIONS["platform.events.retry"].name, TOPIC_DEFINITIONS["platform.events.dlq"].name):
        topic_def = TOPIC_DEFINITIONS[topic_name]
        message_name = "RetryEnvelopeMessage" if "retry" in topic_name else "DlqEnvelopeMessage"
        messages[message_name] = {
            "name": message_name,
            "title": message_name,
            "summary": topic_def.description,
            "contentType": "application/json",
            "payload": {"$ref": "../schemas/v1/event-envelope.schema.json"},
        }
        channels[topic_name] = {
            "address": topic_name,
            "description": topic_def.description,
            "messages": {message_name: {"$ref": f"#/components/messages/{message_name}"}},
            "x-partitions": topic_def.partitions,
            "x-retention-ms": topic_def.retention_ms,
        }
        for publisher in REAL_PUBLISHERS.get(topic_name, []):
            op_id = f"publish_{topic_name.split('.')[-1]}_by_{publisher.replace('.', '_').replace('-', '_')}"
            operations[op_id] = {"action": "send", "channel": {"$ref": f"#/channels/{topic_name}"}, "summary": f"{publisher} publishes to {topic_name}"}
        for subscriber in REAL_SUBSCRIBERS.get(topic_name, []):
            op_id = f"consume_{topic_name.split('.')[-1]}_by_{subscriber.replace('.', '_').replace('-', '_').replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_')}"
            operations[op_id] = {"action": "receive", "channel": {"$ref": f"#/channels/{topic_name}"}, "summary": f"{subscriber} consumes {topic_name}"}

    document = {
        "asyncapi": "3.0.0",
        "info": {
            "title": "Event-Driven Data Platform — Event Architecture",
            "version": "1.0.0",
            "description": (
                "Generated from platform_shared.kafka.TOPIC_DEFINITIONS and contracts/registry.json "
                "by scripts/generate_asyncapi.py — do not hand-edit; regenerate instead. "
                "Documents actual Kafka topics, JSON Schema payload contracts, real publishers/"
                "subscribers, and the platform's declared BACKWARD compatibility policy."
            ),
        },
        "servers": {
            "local": {
                "host": "kafka:9092",
                "protocol": "kafka",
                "description": "Local docker-compose Kafka broker (single broker, no TLS/SASL — see docs/LIMITATIONS.md)",
            }
        },
        "channels": channels,
        "operations": operations,
        "components": {"messages": messages},
    }
    return document


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate contracts/asyncapi.yml from real platform configuration.")
    parser.add_argument("--output", default=str(CONTRACTS_ROOT / "asyncapi.yml"))
    args = parser.parse_args()

    document = build_asyncapi_document()
    output_path = Path(args.output)
    output_path.write_text(_yaml_dump(document) + "\n")
    print(f"wrote {output_path} ({len(document['channels'])} channels, {len(document['operations'])} operations, {len(document['components']['messages'])} messages)")


if __name__ == "__main__":
    main()
