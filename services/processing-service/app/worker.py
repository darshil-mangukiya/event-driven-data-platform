from __future__ import annotations

import asyncio
import logging
from typing import Any

from platform_shared.kafka import TopicNames, create_consumer
from platform_shared.metrics import record_consumer_lag, record_event_processed
from platform_shared.schemas import EventEnvelope, EventType, envelope_from_json
from platform_shared.tracing import extract_trace_context_from_headers, traced_span

from app.processors import (
    metric_delta_for_event,
    processed_order_row,
    processed_payment_row,
    processed_user_session_row,
    product_state_row,
    service_health_row,
    should_raise_risk_alert,
)
from app.repository import ProcessingRepository

LOGGER = logging.getLogger(__name__)


PROCESSING_TOPICS = [
    TopicNames.ORDER_EVENTS,
    TopicNames.PAYMENT_EVENTS,
    TopicNames.USER_EVENTS,
    TopicNames.PRODUCT_EVENTS,
    TopicNames.SYSTEM_EVENTS,
    TopicNames.RETRY_EVENTS,
]


class EventProcessor:
    def __init__(self, repository: ProcessingRepository) -> None:
        self.repository = repository

    async def handle(self, envelope: EventEnvelope) -> None:
        raw_inserted = await self.repository.write_raw_event(envelope)
        if raw_inserted is False:
            LOGGER.info(
                "skipping duplicate event replay",
                extra={"tenant_id": envelope.tenant_id, "event_id": envelope.event_id},
            )
            return
        event_type = EventType(envelope.event_type)

        if event_type in {EventType.ORDER_CREATED, EventType.ORDER_UPDATED}:
            await self.repository.write_processed_order(processed_order_row(envelope))
        elif event_type in {
            EventType.PAYMENT_AUTHORIZED,
            EventType.PAYMENT_CAPTURED,
            EventType.PAYMENT_FAILED,
        }:
            await self.repository.write_processed_payment(processed_payment_row(envelope))
            if should_raise_risk_alert(envelope):
                await self.repository.write_risk_alert(envelope)
        elif event_type in {
            EventType.USER_SIGNED_UP,
            EventType.USER_ACTIVITY,
            EventType.USER_CHURN_SIGNAL,
        }:
            await self.repository.write_user_session_event(processed_user_session_row(envelope))
        elif event_type in {EventType.PRODUCT_UPSERTED, EventType.PRODUCT_INVENTORY_CHANGED}:
            await self.repository.write_product_state(product_state_row(envelope))
        elif event_type in {EventType.SYSTEM_HEALTH, EventType.SYSTEM_ALERT}:
            await self.repository.write_service_health(service_health_row(envelope))

        await self.repository.write_metric_delta(metric_delta_for_event(envelope))
        await self.repository.mark_event_processed(envelope)


class KafkaProcessingWorker:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        group_id: str,
        client_id: str,
        processor: EventProcessor,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.client_id = client_id
        self.processor = processor
        self._consumer: Any | None = None
        self._running = False
        self.records_processed = 0

    async def start(self) -> None:
        self._running = True
        self._consumer = await asyncio.to_thread(
            create_consumer,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            topics=PROCESSING_TOPICS,
            client_id=self.client_id,
        )
        LOGGER.info("Kafka processing worker started")
        while self._running:
            batches = await asyncio.to_thread(self._consumer.poll, timeout_ms=1_000, max_records=250)
            processed_since_commit = 0
            for records in batches.values():
                for record in records:
                    envelope: EventEnvelope | None = None
                    # Continue the producer's trace (see
                    # platform_shared.kafka.KafkaProducer.publish's header
                    # injection) rather than starting a disconnected one —
                    # a no-op when tracing is disabled.
                    header_carrier = {k: v.decode("utf-8") for k, v in (record.headers or []) if v is not None}
                    trace_context = extract_trace_context_from_headers(header_carrier)
                    try:
                        with traced_span(
                            "processing-service",
                            "kafka.consume",
                            {"messaging.source": record.topic, "messaging.system": "kafka"},
                            parent_context=trace_context,
                        ):
                            envelope = envelope_from_json(record.value)
                            await self.processor.handle(envelope)
                            await self.processor.repository.update_watermark(
                                envelope,
                                source_topic=record.topic,
                                last_processed_offset=record.offset,
                            )
                        record_event_processed("processing-service", str(envelope.event_type), "success")
                        self.records_processed += 1
                        processed_since_commit += 1
                    except Exception as exc:
                        event_type = str(envelope.event_type) if envelope else "unknown"
                        record_event_processed("processing-service", event_type, "failed")
                        LOGGER.exception("failed to process Kafka record", extra={"error": str(exc)})
            if processed_since_commit:
                await asyncio.to_thread(self._consumer.commit)
            await asyncio.to_thread(self._record_consumer_lag)

    def _record_consumer_lag(self) -> None:
        """Real Kafka consumer lag: for each assigned partition, the
        broker's current high-water mark minus this consumer's committed
        position. The same signal KEDA's ScaledObject scales on
        (evidence/validation/keda-autoscaling-live-verification.md) — exposed here via
        Prometheus so it's visible without a Kubernetes/KEDA deployment.
        Best-effort: a lag-reporting failure must never crash the actual
        consume loop.
        """
        if self._consumer is None:
            return
        try:
            assignment = self._consumer.assignment()
            if not assignment:
                return
            end_offsets = self._consumer.end_offsets(list(assignment))
            for topic_partition in assignment:
                position = self._consumer.position(topic_partition)
                high_water_mark = end_offsets.get(topic_partition)
                if position is None or high_water_mark is None:
                    continue
                record_consumer_lag(
                    "processing-service", topic_partition.topic, topic_partition.partition, max(0, high_water_mark - position)
                )
        except Exception:  # noqa: BLE001 — best-effort observability, never fail the consume loop over it
            LOGGER.debug("failed to record consumer lag", exc_info=True)

    async def stop(self) -> None:
        self._running = False
        if self._consumer is not None:
            await asyncio.to_thread(self._consumer.close)
