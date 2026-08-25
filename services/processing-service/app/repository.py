from __future__ import annotations

import json
from datetime import timezone
from typing import Any

from platform_shared.database import Postgres
from platform_shared.schemas import EventEnvelope

from app.processors import MetricDelta


class ProcessingRepository:
    def __init__(self, postgres: Postgres) -> None:
        self.postgres = postgres

    async def write_raw_event(self, envelope: EventEnvelope) -> bool:
        inbox_status = await self.postgres.execute_scoped(
            envelope.tenant_id,
            """
            insert into event_inbox (
                consumer_name, event_id, tenant_id, event_type, trace_id, status, received_at
            )
            values ('processing-service', $1, $2, $3, $4, 'processing', now())
            on conflict (consumer_name, event_id) do nothing
            """,
            envelope.event_id,
            envelope.tenant_id,
            str(envelope.event_type),
            envelope.trace_id,
        )
        if not inbox_status.endswith(" 1"):
            return False

        status = await self.postgres.execute_scoped(
            envelope.tenant_id,
            """
            insert into raw_events (
                event_id, tenant_id, event_type, event_timestamp, source_service,
                payload_version, payload, trace_id, correlation_id, causation_id,
                idempotency_key, ingested_at
            )
            values ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11, now())
            on conflict (event_id) do nothing
            """,
            envelope.event_id,
            envelope.tenant_id,
            str(envelope.event_type),
            envelope.event_timestamp,
            envelope.source_service,
            envelope.payload_version,
            json.dumps(envelope.payload),
            envelope.trace_id,
            envelope.correlation_id,
            envelope.causation_id,
            envelope.idempotency_key,
        )
        inserted = status.endswith(" 1")
        if not inserted:
            await self.mark_event_duplicate(envelope, "raw_events already contained event_id")
        return inserted

    async def mark_event_processed(self, envelope: EventEnvelope) -> None:
        await self.postgres.execute_scoped(
            envelope.tenant_id,
            """
            update event_inbox
            set status = 'processed',
                processed_at = now(),
                error_message = null
            where consumer_name = 'processing-service'
              and event_id = $1
              and tenant_id = $2
            """,
            envelope.event_id,
            envelope.tenant_id,
        )

    async def update_watermark(
        self,
        envelope: EventEnvelope,
        *,
        source_topic: str,
        last_processed_offset: int,
        status: str = "active",
    ) -> None:
        await self.postgres.execute_scoped(
            envelope.tenant_id,
            """
            insert into pipeline_watermarks (
                pipeline_name, tenant_id, source_topic, last_processed_timestamp,
                last_processed_offset, status, updated_at, metadata
            )
            values (
                'processing-service', $1, $2, $3, $4, $5, now(),
                jsonb_build_object('event_id', $6::text, 'event_type', $7::text, 'trace_id', $8::text)
            )
            on conflict (pipeline_name, tenant_id, source_topic) do update set
                last_processed_timestamp = excluded.last_processed_timestamp,
                last_processed_offset = greatest(
                    coalesce(pipeline_watermarks.last_processed_offset, -1),
                    excluded.last_processed_offset
                ),
                status = excluded.status,
                updated_at = now(),
                metadata = excluded.metadata
            """,
            envelope.tenant_id,
            source_topic,
            envelope.event_timestamp,
            last_processed_offset,
            status,
            envelope.event_id,
            str(envelope.event_type),
            envelope.trace_id,
        )

    async def mark_event_duplicate(self, envelope: EventEnvelope, reason: str) -> None:
        await self.postgres.execute_scoped(
            envelope.tenant_id,
            """
            update event_inbox
            set status = 'duplicate',
                processed_at = now(),
                error_message = $3
            where consumer_name = 'processing-service'
              and event_id = $1
              and tenant_id = $2
            """,
            envelope.event_id,
            envelope.tenant_id,
            reason,
        )

    async def write_processed_order(self, row: dict[str, Any]) -> None:
        await self.postgres.execute_scoped(
            row["tenant_id"],
            """
            insert into processed_orders (
                tenant_id, event_id, order_id, customer_id, product_id, quantity,
                unit_price, discount_amount, gross_revenue, net_revenue, currency, status,
                channel, marketing_campaign_id, region, event_timestamp
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
            on conflict (tenant_id, order_id) do update set
                quantity = excluded.quantity,
                unit_price = excluded.unit_price,
                discount_amount = excluded.discount_amount,
                gross_revenue = excluded.gross_revenue,
                net_revenue = excluded.net_revenue,
                status = excluded.status,
                channel = excluded.channel,
                marketing_campaign_id = excluded.marketing_campaign_id,
                region = excluded.region,
                event_timestamp = excluded.event_timestamp,
                updated_at = now()
            """,
            row["tenant_id"],
            row["event_id"],
            row["order_id"],
            row["customer_id"],
            row["product_id"],
            row["quantity"],
            row["unit_price"],
            row["discount_amount"],
            row["gross_revenue"],
            row["net_revenue"],
            row["currency"],
            row["status"],
            row["channel"],
            row["marketing_campaign_id"],
            row["region"],
            row["event_timestamp"],
        )

    async def write_processed_payment(self, row: dict[str, Any]) -> None:
        await self.postgres.execute_scoped(
            row["tenant_id"],
            """
            insert into processed_payments (
                tenant_id, event_id, payment_id, order_id, customer_id, amount, currency,
                status, payment_method, failure_code, risk_score, event_timestamp
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            on conflict (tenant_id, payment_id) do update set
                status = excluded.status,
                failure_code = excluded.failure_code,
                risk_score = excluded.risk_score,
                event_timestamp = excluded.event_timestamp,
                updated_at = now()
            """,
            row["tenant_id"],
            row["event_id"],
            row["payment_id"],
            row["order_id"],
            row["customer_id"],
            row["amount"],
            row["currency"],
            row["status"],
            row["payment_method"],
            row["failure_code"],
            row["risk_score"],
            row["event_timestamp"],
        )

    async def write_user_session_event(self, row: dict[str, Any]) -> None:
        await self.postgres.execute_scoped(
            row["tenant_id"],
            """
            insert into processed_user_sessions (
                tenant_id, event_id, user_id, session_id, action, page, referrer,
                duration_seconds, plan, marketing_campaign_id, event_timestamp
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            on conflict (event_id) do nothing
            """,
            row["tenant_id"],
            row["event_id"],
            row["user_id"],
            row["session_id"],
            row["action"],
            row["page"],
            row["referrer"],
            row["duration_seconds"],
            row["plan"],
            row["marketing_campaign_id"],
            row["event_timestamp"],
        )

    async def write_product_state(self, row: dict[str, Any]) -> None:
        await self.postgres.execute_scoped(
            row["tenant_id"],
            """
            insert into tenant_products (
                tenant_id, product_id, sku, name, category, price, inventory_on_hand,
                active, last_event_id, updated_at
            )
            values ($1,$2,$3,$4,$5,$6,greatest($7,0),$8,$9,$10)
            on conflict (tenant_id, product_id) do update set
                sku = excluded.sku,
                name = excluded.name,
                category = excluded.category,
                price = excluded.price,
                inventory_on_hand = greatest(tenant_products.inventory_on_hand + $7, 0),
                active = excluded.active,
                last_event_id = excluded.last_event_id,
                updated_at = excluded.updated_at
            """,
            row["tenant_id"],
            row["product_id"],
            row["sku"],
            row["name"],
            row["category"],
            row["price"],
            row["inventory_delta"],
            row["active"],
            row["event_id"],
            row["event_timestamp"],
        )

    async def write_service_health(self, row: dict[str, Any]) -> None:
        await self.postgres.execute_scoped(
            row["tenant_id"],
            """
            insert into service_health_metrics (
                tenant_id, event_id, service_name, status, latency_ms, error_count,
                throughput_per_minute, kafka_lag, cache_hit_rate, message, event_timestamp
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            on conflict (event_id) do nothing
            """,
            row["tenant_id"],
            row["event_id"],
            row["service_name"],
            row["status"],
            row["latency_ms"],
            row["error_count"],
            row["throughput_per_minute"],
            row["kafka_lag"],
            row["cache_hit_rate"],
            row["message"],
            row["event_timestamp"],
        )

    async def write_metric_delta(self, delta: MetricDelta) -> None:
        await self.postgres.execute_scoped(
            delta.tenant_id,
            """
            insert into tenant_metrics_hourly (
                tenant_id, metric_hour, gross_revenue, net_revenue, order_count, units_sold,
                new_users, active_users, churn_signal_count, payment_success_count,
                payment_failure_count, marketing_spend, marketing_attributed_revenue,
                events_processed, updated_at
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,now())
            on conflict (tenant_id, metric_hour) do update set
                gross_revenue = tenant_metrics_hourly.gross_revenue + excluded.gross_revenue,
                net_revenue = tenant_metrics_hourly.net_revenue + excluded.net_revenue,
                order_count = tenant_metrics_hourly.order_count + excluded.order_count,
                units_sold = tenant_metrics_hourly.units_sold + excluded.units_sold,
                new_users = tenant_metrics_hourly.new_users + excluded.new_users,
                active_users = tenant_metrics_hourly.active_users + excluded.active_users,
                churn_signal_count = tenant_metrics_hourly.churn_signal_count + excluded.churn_signal_count,
                payment_success_count = tenant_metrics_hourly.payment_success_count + excluded.payment_success_count,
                payment_failure_count = tenant_metrics_hourly.payment_failure_count + excluded.payment_failure_count,
                marketing_spend = tenant_metrics_hourly.marketing_spend + excluded.marketing_spend,
                marketing_attributed_revenue = tenant_metrics_hourly.marketing_attributed_revenue + excluded.marketing_attributed_revenue,
                events_processed = tenant_metrics_hourly.events_processed + excluded.events_processed,
                updated_at = now()
            """,
            delta.tenant_id,
            delta.metric_ts.astimezone(timezone.utc),
            delta.gross_revenue,
            delta.net_revenue,
            delta.order_count,
            delta.units_sold,
            delta.new_users,
            delta.active_users,
            delta.churn_signal_count,
            delta.payment_success_count,
            delta.payment_failure_count,
            delta.marketing_spend,
            delta.marketing_attributed_revenue,
            delta.events_processed,
        )
        await self.postgres.execute_scoped(
            delta.tenant_id,
            """
            insert into tenant_metrics_daily (
                tenant_id, metric_date, gross_revenue, net_revenue, order_count, units_sold,
                new_users, active_users, churn_signal_count, payment_success_count,
                payment_failure_count, marketing_spend, marketing_attributed_revenue,
                events_processed, updated_at
            )
            values ($1,$2::date,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,now())
            on conflict (tenant_id, metric_date) do update set
                gross_revenue = tenant_metrics_daily.gross_revenue + excluded.gross_revenue,
                net_revenue = tenant_metrics_daily.net_revenue + excluded.net_revenue,
                order_count = tenant_metrics_daily.order_count + excluded.order_count,
                units_sold = tenant_metrics_daily.units_sold + excluded.units_sold,
                new_users = tenant_metrics_daily.new_users + excluded.new_users,
                active_users = tenant_metrics_daily.active_users + excluded.active_users,
                churn_signal_count = tenant_metrics_daily.churn_signal_count + excluded.churn_signal_count,
                payment_success_count = tenant_metrics_daily.payment_success_count + excluded.payment_success_count,
                payment_failure_count = tenant_metrics_daily.payment_failure_count + excluded.payment_failure_count,
                marketing_spend = tenant_metrics_daily.marketing_spend + excluded.marketing_spend,
                marketing_attributed_revenue = tenant_metrics_daily.marketing_attributed_revenue + excluded.marketing_attributed_revenue,
                events_processed = tenant_metrics_daily.events_processed + excluded.events_processed,
                updated_at = now()
            """,
            delta.tenant_id,
            delta.metric_ts.date(),
            delta.gross_revenue,
            delta.net_revenue,
            delta.order_count,
            delta.units_sold,
            delta.new_users,
            delta.active_users,
            delta.churn_signal_count,
            delta.payment_success_count,
            delta.payment_failure_count,
            delta.marketing_spend,
            delta.marketing_attributed_revenue,
            delta.events_processed,
        )

    async def write_risk_alert(self, envelope: EventEnvelope) -> None:
        await self.postgres.execute_scoped(
            envelope.tenant_id,
            """
            insert into alerts (
                tenant_id, alert_type, severity, status, message, source_event_id, created_at
            )
            values ($1, 'payment_risk', 'high', 'open', $2, $3, now())
            """,
            envelope.tenant_id,
            f"High-risk payment failure for order {envelope.payload.get('order_id')}",
            envelope.event_id,
        )

    async def log_pipeline_run(
        self,
        *,
        pipeline_name: str,
        status: str,
        records_processed: int,
        error_message: str | None = None,
    ) -> None:
        # pipeline_run_log is a platform-wide operational log, not
        # tenant-scoped data (not in tenant_rls.sql's protected table
        # set) — intentionally left as a plain, unscoped write.
        await self.postgres.execute(
            """
            insert into pipeline_run_log (
                pipeline_name, status, records_processed, error_message, started_at, finished_at
            )
            values ($1,$2,$3,$4,now(),now())
            """,
            pipeline_name,
            status,
            records_processed,
            error_message,
        )
