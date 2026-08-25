from __future__ import annotations

from datetime import date
from typing import Any

from platform_shared.database import Postgres


class AnalyticsRepository:
    def __init__(self, postgres: Postgres) -> None:
        self.postgres = postgres

    @staticmethod
    def _window_clause(
        tenant_id: str,
        column_name: str,
        start_date: date | None,
        end_date: date | None,
        extra_where: str = "",
    ) -> tuple[str, list[Any]]:
        params: list[Any] = [tenant_id]
        clauses = ["tenant_id = $1"]
        if start_date:
            params.append(start_date)
            clauses.append(f"{column_name} >= ${len(params)}")
        if end_date:
            params.append(end_date)
            clauses.append(f"{column_name} <= ${len(params)}")
        if extra_where:
            clauses.append(extra_where)
        return " and ".join(clauses), params

    async def revenue(
        self,
        *,
        tenant_id: str,
        start_date: date | None,
        end_date: date | None,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        where_sql, params = self._window_clause(tenant_id, "metric_date", start_date, end_date)
        params.extend([limit, offset])
        return await self.postgres.fetch_scoped(
            tenant_id,
            f"""
            select metric_date, gross_revenue, net_revenue, order_count, units_sold,
                   round(net_revenue / nullif(order_count, 0), 2) as average_order_value
            from tenant_metrics_daily
            where {where_sql}
            order by metric_date desc
            limit ${len(params)-1} offset ${len(params)}
            """,
            *params,
        )

    async def customers(
        self,
        *,
        tenant_id: str,
        start_date: date | None,
        end_date: date | None,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        where_sql, params = self._window_clause(tenant_id, "metric_date", start_date, end_date)
        params.extend([limit, offset])
        return await self.postgres.fetch_scoped(
            tenant_id,
            f"""
            select metric_date, new_users, active_users,
                   sum(new_users) over (partition by tenant_id order by metric_date) as cumulative_customers,
                   churn_signal_count
            from tenant_metrics_daily
            where {where_sql}
            order by metric_date desc
            limit ${len(params)-1} offset ${len(params)}
            """,
            *params,
        )

    async def churn(
        self,
        *,
        tenant_id: str,
        start_date: date | None,
        end_date: date | None,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        where_sql, params = self._window_clause(tenant_id, "metric_date", start_date, end_date)
        params.extend([limit, offset])
        return await self.postgres.fetch_scoped(
            tenant_id,
            f"""
            select metric_date, churn_signal_count, active_users,
                   round(churn_signal_count::numeric / nullif(active_users, 0), 4) as churn_signal_rate
            from tenant_metrics_daily
            where {where_sql}
            order by metric_date desc
            limit ${len(params)-1} offset ${len(params)}
            """,
            *params,
        )

    async def retention(
        self,
        *,
        tenant_id: str,
        start_date: date | None,
        end_date: date | None,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        where_sql, params = self._window_clause(tenant_id, "metric_date", start_date, end_date)
        params.extend([limit, offset])
        return await self.postgres.fetch_scoped(
            tenant_id,
            f"""
            select metric_date, active_users, churn_signal_count,
                   greatest(0, 1 - coalesce(churn_signal_count::numeric / nullif(active_users, 0), 0)) as estimated_retention_rate
            from tenant_metrics_daily
            where {where_sql}
            order by metric_date desc
            limit ${len(params)-1} offset ${len(params)}
            """,
            *params,
        )

    async def marketing_roi(
        self,
        *,
        tenant_id: str,
        start_date: date | None,
        end_date: date | None,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        where_sql, params = self._window_clause(tenant_id, "event_timestamp::date", start_date, end_date)
        params.extend([limit, offset])
        return await self.postgres.fetch_scoped(
            tenant_id,
            f"""
            select coalesce(marketing_campaign_id, 'unattributed') as marketing_campaign_id,
                   count(*) as orders,
                   round(sum(net_revenue), 2) as attributed_revenue,
                   round(count(*) * 3.50, 2) as modeled_spend,
                   round((sum(net_revenue) - count(*) * 3.50) / nullif(count(*) * 3.50, 0), 4) as roi
            from processed_orders
            where {where_sql}
            group by coalesce(marketing_campaign_id, 'unattributed')
            order by attributed_revenue desc
            limit ${len(params)-1} offset ${len(params)}
            """,
            *params,
        )

    async def product_performance(
        self,
        *,
        tenant_id: str,
        start_date: date | None,
        end_date: date | None,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        params: list[Any] = [tenant_id]
        clauses = ["o.tenant_id = $1"]
        if start_date:
            params.append(start_date)
            clauses.append(f"o.event_timestamp::date >= ${len(params)}")
        if end_date:
            params.append(end_date)
            clauses.append(f"o.event_timestamp::date <= ${len(params)}")
        where_sql = " and ".join(clauses)
        params.extend([limit, offset])
        return await self.postgres.fetch_scoped(
            tenant_id,
            f"""
            select o.product_id,
                   coalesce(p.name, o.product_id) as product_name,
                   coalesce(p.category, 'unknown') as category,
                   count(*) as orders,
                   sum(o.quantity) as units_sold,
                   round(sum(o.net_revenue), 2) as net_revenue,
                   round(sum(o.net_revenue) / nullif(sum(o.quantity), 0), 2) as revenue_per_unit
            from processed_orders o
            left join tenant_products p
              on p.tenant_id = o.tenant_id and p.product_id = o.product_id
            where {where_sql}
            group by o.product_id, coalesce(p.name, o.product_id), coalesce(p.category, 'unknown')
            order by net_revenue desc
            limit ${len(params)-1} offset ${len(params)}
            """,
            *params,
        )

    async def payment_success(
        self,
        *,
        tenant_id: str,
        start_date: date | None,
        end_date: date | None,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        where_sql, params = self._window_clause(tenant_id, "metric_date", start_date, end_date)
        params.extend([limit, offset])
        return await self.postgres.fetch_scoped(
            tenant_id,
            f"""
            select metric_date,
                   payment_success_count,
                   payment_failure_count,
                   payment_success_count + payment_failure_count as payment_attempt_count,
                   round(
                       payment_success_count::numeric
                       / nullif(payment_success_count + payment_failure_count, 0),
                       4
                   ) as payment_success_rate
            from tenant_metrics_daily
            where {where_sql}
            order by metric_date desc
            limit ${len(params)-1} offset ${len(params)}
            """,
            *params,
        )

    async def event_throughput(
        self,
        *,
        tenant_id: str,
        start_date: date | None,
        end_date: date | None,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        params: list[Any] = [tenant_id]
        clauses = ["m.tenant_id = $1"]
        if start_date:
            params.append(start_date)
            clauses.append(f"m.metric_date >= ${len(params)}")
        if end_date:
            params.append(end_date)
            clauses.append(f"m.metric_date <= ${len(params)}")
        where_sql = " and ".join(clauses)
        params.extend([limit, offset])
        return await self.postgres.fetch_scoped(
            tenant_id,
            f"""
            select m.metric_date,
                   m.events_processed,
                   (
                       select count(*)
                       from raw_events r
                       where r.tenant_id = m.tenant_id
                         and r.event_timestamp::date = m.metric_date
                   ) as raw_event_count,
                   m.order_count,
                   m.payment_success_count,
                   m.payment_failure_count,
                   m.updated_at
            from tenant_metrics_daily m
            where {where_sql}
            order by m.metric_date desc
            limit ${len(params)-1} offset ${len(params)}
            """,
            *params,
        )

    async def tenant_health_score(self, *, tenant_id: str) -> dict[str, Any]:
        return await self.postgres.fetchrow_scoped(
            tenant_id,
            """
            with latest as (
                select *
                from tenant_metrics_daily
                where tenant_id = $1
                order by metric_date desc
                limit 7
            ),
            health as (
                select
                    coalesce(sum(events_processed), 0) as events_processed,
                    coalesce(sum(payment_failure_count), 0) as payment_failures,
                    coalesce(sum(payment_success_count), 0) as payment_successes,
                    coalesce(avg(churn_signal_count::numeric / nullif(active_users, 0)), 0) as churn_rate
                from latest
            )
            select
                $1 as tenant_id,
                events_processed,
                payment_successes,
                payment_failures,
                round(greatest(0, least(100,
                    100
                    - (payment_failures::numeric / nullif(payment_successes + payment_failures, 0)) * 35
                    - churn_rate * 50
                )), 2) as tenant_health_score
            from health
            """,
            tenant_id,
        ) or {"tenant_id": tenant_id, "tenant_health_score": 0}

    async def alerts(self, *, tenant_id: str, limit: int, offset: int) -> list[dict[str, Any]]:
        return await self.postgres.fetch_scoped(
            tenant_id,
            """
            select alert_id, alert_type, severity, status, message, source_event_id, created_at
            from alerts
            where tenant_id = $1
            order by created_at desc
            limit $2 offset $3
            """,
            tenant_id,
            limit,
            offset,
        )

    async def tenants(self) -> list[dict[str, Any]]:
        # Intentionally cross-tenant and unscoped: tenant_config is the
        # platform's own tenant catalog, not tenant-scoped data (not in
        # tenant_rls.sql's protected table set), and this endpoint is the
        # legitimate way to list all tenants.
        return await self.postgres.fetch(
            """
            select tenant_id, tenant_name, plan, region, is_active, created_at
            from tenant_config
            order by tenant_name
            """
        )

    async def system_status(self, *, tenant_id: str) -> dict[str, Any]:
        service_health = await self.postgres.fetch_scoped(
            tenant_id,
            """
            select distinct on (service_name) service_name, status, latency_ms, error_count,
                   throughput_per_minute, kafka_lag, cache_hit_rate, event_timestamp
            from service_health_metrics
            where tenant_id = $1
            order by service_name, event_timestamp desc
            """,
            tenant_id,
        )
        # pipeline_run_log is a platform-wide operational log, not a
        # tenant-scoped table (not in tenant_rls.sql's protected set) — it
        # intentionally stays a plain, unscoped query.
        pipeline_runs = await self.postgres.fetch(
            """
            select pipeline_name, status, records_processed, error_message, started_at, finished_at
            from pipeline_run_log
            order by started_at desc
            limit 10
            """
        )
        return {"service_health": service_health, "recent_pipeline_runs": pipeline_runs}
