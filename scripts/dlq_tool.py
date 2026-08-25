from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from platform_shared.config import service_settings
from platform_shared.database import Postgres
from platform_shared.kafka import KafkaEventProducer, TopicNames, TopicRouter, create_consumer
from platform_shared.schemas import EventEnvelope, envelope_from_json


@dataclass(frozen=True)
class DlqRecord:
    envelope: EventEnvelope
    original_event: EventEnvelope | None
    error: str | None
    failed_stage: str | None
    topic: str
    partition: int
    offset: int


def extract_dlq_record(raw_value: str, *, topic: str, partition: int, offset: int) -> DlqRecord:
    try:
        envelope = envelope_from_json(raw_value)
    except ValueError:
        # Spark's streaming DLQ preserves the raw original envelope instead
        # of wrapping it in the processing service's system.alert envelope.
        # Normalize that project-native shape so one operator tool can inspect
        # and replay records emitted by either producer.
        payload = json.loads(raw_value)
        if not isinstance(payload, dict) or not payload.get("raw_value"):
            raise
        original_event = envelope_from_json(str(payload["raw_value"]))
        return DlqRecord(
            envelope=original_event,
            original_event=original_event,
            error=str(payload.get("rejection_reason") or "spark_stream_rejected"),
            failed_stage="spark-streaming",
            topic=topic,
            partition=partition,
            offset=offset,
        )
    original_event: EventEnvelope | None = None
    error: str | None = None
    failed_stage: str | None = None

    message = envelope.payload.get("message")
    if message:
        try:
            parsed = json.loads(str(message))
            error = parsed.get("error")
            failed_stage = parsed.get("failed_stage")
            if parsed.get("original_event"):
                original_event = EventEnvelope.model_validate(parsed["original_event"])
        except (json.JSONDecodeError, ValueError, TypeError):
            error = str(message)

    if original_event is None and envelope.payload.get("original_event"):
        original_event = EventEnvelope.model_validate(envelope.payload["original_event"])

    return DlqRecord(
        envelope=envelope,
        original_event=original_event,
        error=error,
        failed_stage=failed_stage,
        topic=topic,
        partition=partition,
        offset=offset,
    )


def read_dlq_records(args: argparse.Namespace) -> list[DlqRecord]:
    settings = service_settings("dlq-tool")
    consumer = create_consumer(
        bootstrap_servers=args.bootstrap_servers or settings.kafka_bootstrap_servers,
        group_id=args.group_id,
        topics=[TopicNames.DLQ_EVENTS],
        client_id="dlq-tool",
    )
    records: list[DlqRecord] = []
    empty_polls = 0
    try:
        while len(records) < args.max_records and empty_polls < 3:
            batches = consumer.poll(timeout_ms=args.timeout_ms, max_records=args.max_records)
            if not batches:
                # A new consumer group may spend its first poll joining and
                # receiving an assignment. Allow bounded empty polls before
                # concluding that the DLQ has no matching records.
                empty_polls += 1
                continue
            empty_polls = 0
            for topic_partition, messages in batches.items():
                for message in messages:
                    record = extract_dlq_record(
                        message.value,
                        topic=topic_partition.topic,
                        partition=topic_partition.partition,
                        offset=message.offset,
                    )
                    if args.event_id and record.envelope.event_id != args.event_id:
                        if not record.original_event or record.original_event.event_id != args.event_id:
                            continue
                    records.append(record)
                    if len(records) >= args.max_records:
                        break
    finally:
        consumer.close()
    return records


async def audit_replay(
    *,
    database_url: str,
    record: DlqRecord,
    status: str,
    target_topic: str | None,
    replayed_by: str,
    reason: str | None,
    error_message: str | None = None,
) -> None:
    postgres = Postgres(database_url)
    await postgres.execute(
        """
        insert into dlq_replay_audit (
            original_event_id, tenant_id, source_topic, target_topic, replay_status,
            replay_reason, replayed_by, error_message
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8)
        """,
        record.original_event.event_id if record.original_event else record.envelope.event_id,
        record.original_event.tenant_id if record.original_event else record.envelope.tenant_id,
        record.topic,
        target_topic,
        status,
        reason,
        replayed_by,
        error_message,
    )
    await postgres.close()


def inspect(args: argparse.Namespace) -> None:
    records = read_dlq_records(args)
    for record in records:
        payload: dict[str, Any] = {
            "dlq_event_id": record.envelope.event_id,
            "original_event_id": record.original_event.event_id if record.original_event else None,
            "tenant_id": record.original_event.tenant_id if record.original_event else record.envelope.tenant_id,
            "original_event_type": str(record.original_event.event_type) if record.original_event else None,
            "failed_stage": record.failed_stage,
            "error": record.error,
            "topic": record.topic,
            "partition": record.partition,
            "offset": record.offset,
        }
        print(json.dumps(payload, sort_keys=True))


async def replay(args: argparse.Namespace) -> None:
    settings = service_settings("dlq-tool")
    records = read_dlq_records(args)
    if not records:
        print(json.dumps({"status": "no_records", "event_id": args.event_id}))
        return

    producer = KafkaEventProducer(
        bootstrap_servers=args.bootstrap_servers or settings.kafka_bootstrap_servers,
        client_id="dlq-tool",
    )
    router = TopicRouter()
    database_url = args.database_url or os.getenv("DATABASE_URL")
    replayed = 0
    failed = 0

    for record in records:
        if record.original_event is None:
            failed += 1
            if database_url:
                await audit_replay(
                    database_url=database_url,
                    record=record,
                    status="skipped",
                    target_topic=None,
                    replayed_by=args.replayed_by,
                    reason=args.reason,
                    error_message="No original_event was found in DLQ payload.",
                )
            continue

        original_event = record.original_event
        if args.event_timestamp_now:
            original_event = original_event.model_copy(
                update={"event_timestamp": datetime.now(timezone.utc)}
            )
        if args.new_event_id:
            original_event = original_event.model_copy(
                update={
                    "event_id": args.new_event_id,
                    "idempotency_key": args.new_event_id,
                    "causation_id": record.original_event.event_id,
                }
            )
        target_topic = args.target_topic or router.route(original_event.event_type)
        if args.dry_run:
            status = "dry_run"
        else:
            try:
                producer.publish(original_event, topic=target_topic)
                status = "replayed"
            except Exception as exc:
                failed += 1
                status = "failed"
                if database_url:
                    await audit_replay(
                        database_url=database_url,
                        record=record,
                        status=status,
                        target_topic=target_topic,
                        replayed_by=args.replayed_by,
                        reason=args.reason,
                        error_message=str(exc),
                    )
                continue

        replayed += 1
        if database_url:
            await audit_replay(
                database_url=database_url,
                record=record,
                status=status,
                target_topic=target_topic,
                replayed_by=args.replayed_by,
                reason=args.reason,
            )

    print(json.dumps({"status": "complete", "replayed": replayed, "failed": failed, "dry_run": args.dry_run}))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect and replay Kafka DLQ records.")
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--group-id", default="dlq-tool")
    parser.add_argument("--max-records", type=int, default=10)
    parser.add_argument("--timeout-ms", type=int, default=3000)
    parser.add_argument("--event-id", default=None)

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("inspect")

    replay_parser = subparsers.add_parser("replay")
    replay_parser.add_argument("--target-topic", default=None)
    replay_parser.add_argument("--database-url", default=None)
    replay_parser.add_argument("--replayed-by", default=os.getenv("USER", "local-operator"))
    replay_parser.add_argument("--reason", default="manual replay")
    replay_parser.add_argument("--dry-run", action="store_true")
    replay_parser.add_argument(
        "--event-timestamp-now",
        action="store_true",
        help="Operator correction for a late event: replay it with current event time.",
    )
    replay_parser.add_argument(
        "--new-event-id",
        default=None,
        help="Publish a corrected successor ID and retain the original ID as causation_id.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "inspect":
        inspect(args)
    elif args.command == "replay":
        asyncio.run(replay(args))


if __name__ == "__main__":
    main()
