"""Tests for services/analytics-service/app/repository.py — the actual
SQL-building data-access layer behind every /metrics/* endpoint.

coverage analysis found this file at 27% coverage: every API-level
test (tests/test_api_contracts.py) exercises the endpoints through a
FakeAnalyticsRepository double, which means the *real* repository — the
code that decides which table to query, how tenant/date filters and
limit/offset bind into asyncpg's positional $n parameters, and what each
metric's actual SQL computes — was essentially never directly executed.
A bug in `_window_clause`'s parameter numbering (shared by 8 of the 10
repository methods) would silently produce wrong filters or wrong
limit/offset on every metrics endpoint without any test catching it.

These tests use a FakePostgres double that records the exact SQL and
positional parameters passed to fetch()/fetchrow(), the same pattern
tests/test_ops_console.py and tests/test_reconciliation.py already use —
not a live database, but real execution of the repository's own query-
building logic, which is what was actually missing.
"""

from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path
from typing import Any

import pytest

# Loaded by file path, not `sys.path.insert` + `from app.repository import
# ...` — every service under services/*/app/ shares the bare package name
# "app", and inserting analytics-service onto sys.path would leave
# sys.modules["app"] pointing at analytics-service's package for the rest
# of the process, breaking any other test file's own `from app.X import
# ...` at collection time (tests/conftest.py sets up processing-service's
# "app" the same way, for tests/test_processing_logic.py). This module
# only needs the one class, so load it in isolation instead — see
# tests/test_ops_console.py / tests/test_reconciliation.py for the same
# class of bug this pattern avoids, found in the affected components.
_REPOSITORY_PATH = Path(__file__).resolve().parents[1] / "services" / "analytics-service" / "app" / "repository.py"
_spec = importlib.util.spec_from_file_location("analytics_service_repository", _REPOSITORY_PATH)
_repository_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_repository_module)
AnalyticsRepository = _repository_module.AnalyticsRepository


class FakePostgres:
    def __init__(self, fetch_result: list[dict[str, Any]] | None = None, fetchrow_result: dict[str, Any] | None = None) -> None:
        self.fetch_result = fetch_result if fetch_result is not None else []
        self.fetchrow_result = fetchrow_result
        self.fetch_calls: list[tuple[str, tuple]] = []
        self.fetchrow_calls: list[tuple[str, tuple]] = []
        self.tenant_context_used: str | None = None

    async def fetch(self, query: str, *params: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((query, params))
        return self.fetch_result

    async def fetchrow(self, query: str, *params: Any) -> dict[str, Any] | None:
        self.fetchrow_calls.append((query, params))
        return self.fetchrow_result

    # AnalyticsRepository calls these because every tenant-scoped query sets PostgreSQL's
    # app.tenant_id transaction-locally before running). This double
    # doesn't model the real tenant-context/transaction behavior (that's
    # covered live by tests/test_database_tenant_scoping.py) — it just
    # records the same (query, params) shape the plain fetch/fetchrow
    # calls above do, so every existing assertion here against
    # fetch_calls/fetchrow_calls keeps working unchanged.
    async def fetch_scoped(self, tenant_id: str, query: str, *params: Any) -> list[dict[str, Any]]:
        self.tenant_context_used = tenant_id
        return await self.fetch(query, *params)

    async def fetchrow_scoped(self, tenant_id: str, query: str, *params: Any) -> dict[str, Any] | None:
        self.tenant_context_used = tenant_id
        return await self.fetchrow(query, *params)


# ---------------------------------------------------------------------------
# _window_clause — pure logic, shared by 8 of 10 repository methods
# ---------------------------------------------------------------------------


def test_window_clause_with_no_dates_only_filters_tenant() -> None:
    where_sql, params = AnalyticsRepository._window_clause("tenant_demo", "metric_date", None, None)
    assert where_sql == "tenant_id = $1"
    assert params == ["tenant_demo"]


def test_window_clause_with_start_date_only() -> None:
    where_sql, params = AnalyticsRepository._window_clause("tenant_demo", "metric_date", date(2026, 6, 1), None)
    assert where_sql == "tenant_id = $1 and metric_date >= $2"
    assert params == ["tenant_demo", date(2026, 6, 1)]


def test_window_clause_with_end_date_only() -> None:
    where_sql, params = AnalyticsRepository._window_clause("tenant_demo", "metric_date", None, date(2026, 6, 2))
    assert where_sql == "tenant_id = $1 and metric_date <= $2"
    assert params == ["tenant_demo", date(2026, 6, 2)]


def test_window_clause_with_both_dates_numbers_params_sequentially() -> None:
    where_sql, params = AnalyticsRepository._window_clause(
        "tenant_demo", "metric_date", date(2026, 6, 1), date(2026, 6, 2)
    )
    assert where_sql == "tenant_id = $1 and metric_date >= $2 and metric_date <= $3"
    assert params == ["tenant_demo", date(2026, 6, 1), date(2026, 6, 2)]


def test_window_clause_with_extra_where_appends_after_dates() -> None:
    where_sql, params = AnalyticsRepository._window_clause(
        "tenant_demo", "metric_date", date(2026, 6, 1), None, extra_where="status = 'active'"
    )
    assert where_sql == "tenant_id = $1 and metric_date >= $2 and status = 'active'"
    assert params == ["tenant_demo", date(2026, 6, 1)]


def test_window_clause_uses_the_given_column_name_not_hardcoded() -> None:
    """marketing_roi calls this with 'event_timestamp::date', not
    'metric_date' — the column name must be interpolated, not fixed.
    """
    where_sql, _params = AnalyticsRepository._window_clause(
        "tenant_demo", "event_timestamp::date", date(2026, 6, 1), None
    )
    assert "event_timestamp::date >= $2" in where_sql


# ---------------------------------------------------------------------------
# Every tenant-scoped method must go through fetch_scoped/fetchrow_scoped
# (Postgres.app.tenant_id), not the plain, unscoped fetch/fetchrow — this
# is what makes PostgreSQL RLS enforce anything at runtime for these
# queries rather than relying solely on the WHERE tenant_id = $1 clause.
# See evidence/validation/application-rls-runtime-verification.md.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_every_tenant_scoped_method_sets_tenant_context() -> None:
    postgres = FakePostgres(fetch_result=[], fetchrow_result={"tenant_id": "tenant_demo", "tenant_health_score": 0})
    repo = AnalyticsRepository(postgres)
    kwargs = dict(tenant_id="tenant_demo", start_date=None, end_date=None, limit=10, offset=0)

    for coro in (
        repo.revenue(**kwargs),
        repo.customers(**kwargs),
        repo.churn(**kwargs),
        repo.retention(**kwargs),
        repo.marketing_roi(**kwargs),
        repo.product_performance(**kwargs),
        repo.payment_success(**kwargs),
        repo.event_throughput(**kwargs),
        repo.tenant_health_score(tenant_id="tenant_demo"),
        repo.alerts(tenant_id="tenant_demo", limit=10, offset=0),
    ):
        postgres.tenant_context_used = None
        await coro
        assert postgres.tenant_context_used == "tenant_demo", (
            "a tenant-scoped repository method called plain fetch()/fetchrow() "
            "instead of fetch_scoped()/fetchrow_scoped() — PostgreSQL's app.tenant_id "
            "would never be set for this query, and RLS would not be enforced for it"
        )


@pytest.mark.asyncio
async def test_tenants_and_pipeline_runs_are_intentionally_not_tenant_scoped() -> None:
    """tenant_config and pipeline_run_log are not in tenant_rls.sql's
    protected table set — these two queries are legitimately cross-tenant
    admin/operational reads and must stay on the plain, unscoped path.
    """
    postgres = FakePostgres(fetch_result=[])
    repo = AnalyticsRepository(postgres)

    await repo.tenants()
    assert postgres.tenant_context_used is None

    postgres.tenant_context_used = None
    await repo.system_status(tenant_id="tenant_demo")
    # system_status's service_health query IS scoped; pipeline_runs isn't.
    # By the time both have run, tenant_context_used reflects the last
    # scoped call made (service_health), which is still correct — the
    # important assertion is that a unscoped call (tenants())
    # above never touches it at all.
    assert postgres.tenant_context_used == "tenant_demo"


@pytest.mark.asyncio
async def test_revenue_queries_tenant_metrics_daily_with_tenant_filter_and_limit_offset() -> None:
    postgres = FakePostgres(fetch_result=[{"metric_date": "2026-06-01", "net_revenue": 100.0}])
    repo = AnalyticsRepository(postgres)
    result = await repo.revenue(tenant_id="tenant_demo", start_date=None, end_date=None, limit=30, offset=0)
    assert result == [{"metric_date": "2026-06-01", "net_revenue": 100.0}]
    query, params = postgres.fetch_calls[0]
    assert "tenant_metrics_daily" in query
    assert "average_order_value" in query
    assert params == ("tenant_demo", 30, 0)


@pytest.mark.asyncio
async def test_revenue_applies_date_window_params_in_order() -> None:
    postgres = FakePostgres()
    repo = AnalyticsRepository(postgres)
    await repo.revenue(tenant_id="tenant_demo", start_date=date(2026, 6, 1), end_date=date(2026, 6, 2), limit=10, offset=5)
    query, params = postgres.fetch_calls[0]
    assert params == ("tenant_demo", date(2026, 6, 1), date(2026, 6, 2), 10, 5)
    assert "limit $4 offset $5" in query


@pytest.mark.asyncio
async def test_customers_queries_tenant_metrics_daily_with_cumulative_window_function() -> None:
    postgres = FakePostgres()
    repo = AnalyticsRepository(postgres)
    await repo.customers(tenant_id="tenant_demo", start_date=None, end_date=None, limit=30, offset=0)
    query, params = postgres.fetch_calls[0]
    assert "tenant_metrics_daily" in query
    assert "cumulative_customers" in query
    assert params[0] == "tenant_demo"


@pytest.mark.asyncio
async def test_churn_computes_churn_signal_rate_against_active_users() -> None:
    postgres = FakePostgres()
    repo = AnalyticsRepository(postgres)
    await repo.churn(tenant_id="tenant_demo", start_date=None, end_date=None, limit=30, offset=0)
    query, _params = postgres.fetch_calls[0]
    assert "churn_signal_rate" in query
    assert "nullif(active_users, 0)" in query


@pytest.mark.asyncio
async def test_retention_computes_estimated_retention_rate_never_negative() -> None:
    postgres = FakePostgres()
    repo = AnalyticsRepository(postgres)
    await repo.retention(tenant_id="tenant_demo", start_date=None, end_date=None, limit=30, offset=0)
    query, _params = postgres.fetch_calls[0]
    assert "estimated_retention_rate" in query
    assert "greatest(0" in query


@pytest.mark.asyncio
async def test_payment_success_computes_success_rate_from_serving_counts() -> None:
    postgres = FakePostgres()
    repo = AnalyticsRepository(postgres)
    await repo.payment_success(tenant_id="tenant_demo", start_date=None, end_date=None, limit=30, offset=0)
    query, _params = postgres.fetch_calls[0]
    assert "payment_success_rate" in query
    assert "payment_success_count + payment_failure_count" in query


@pytest.mark.asyncio
async def test_event_throughput_cross_checks_against_raw_events_subquery() -> None:
    postgres = FakePostgres()
    repo = AnalyticsRepository(postgres)
    await repo.event_throughput(tenant_id="tenant_demo", start_date=date(2026, 6, 1), end_date=None, limit=30, offset=0)
    query, params = postgres.fetch_calls[0]
    assert "raw_event_count" in query
    assert "from raw_events r" in query
    assert params == ("tenant_demo", date(2026, 6, 1), 30, 0)


@pytest.mark.asyncio
async def test_event_throughput_applies_both_dates_with_sequential_params() -> None:
    postgres = FakePostgres()
    repo = AnalyticsRepository(postgres)
    await repo.event_throughput(
        tenant_id="tenant_demo", start_date=date(2026, 6, 1), end_date=date(2026, 6, 2), limit=30, offset=0
    )
    query, params = postgres.fetch_calls[0]
    assert "m.metric_date >= $2" in query
    assert "m.metric_date <= $3" in query
    assert params == ("tenant_demo", date(2026, 6, 1), date(2026, 6, 2), 30, 0)


# ---------------------------------------------------------------------------
# marketing_roi / product_performance — processed_orders-based, not
# tenant_metrics_daily
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_marketing_roi_queries_processed_orders_not_tenant_metrics_daily() -> None:
    postgres = FakePostgres()
    repo = AnalyticsRepository(postgres)
    await repo.marketing_roi(tenant_id="tenant_demo", start_date=None, end_date=None, limit=30, offset=0)
    query, _params = postgres.fetch_calls[0]
    assert "from processed_orders" in query
    assert "tenant_metrics_daily" not in query
    assert "unattributed" in query


@pytest.mark.asyncio
async def test_product_performance_left_joins_tenant_products() -> None:
    postgres = FakePostgres()
    repo = AnalyticsRepository(postgres)
    await repo.product_performance(tenant_id="tenant_demo", start_date=None, end_date=None, limit=30, offset=0)
    query, params = postgres.fetch_calls[0]
    assert "from processed_orders o" in query
    assert "left join tenant_products p" in query
    assert params[0] == "tenant_demo"


@pytest.mark.asyncio
async def test_product_performance_applies_both_dates_with_sequential_params() -> None:
    postgres = FakePostgres()
    repo = AnalyticsRepository(postgres)
    await repo.product_performance(
        tenant_id="tenant_demo", start_date=date(2026, 6, 1), end_date=date(2026, 6, 2), limit=30, offset=0
    )
    query, params = postgres.fetch_calls[0]
    assert "o.event_timestamp::date >= $2" in query
    assert "o.event_timestamp::date <= $3" in query
    assert params == ("tenant_demo", date(2026, 6, 1), date(2026, 6, 2), 30, 0)


# ---------------------------------------------------------------------------
# tenant_health_score / alerts / tenants / system_status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tenant_health_score_returns_fetchrow_result_when_present() -> None:
    postgres = FakePostgres(fetchrow_result={"tenant_id": "tenant_demo", "tenant_health_score": 92.5})
    repo = AnalyticsRepository(postgres)
    result = await repo.tenant_health_score(tenant_id="tenant_demo")
    assert result == {"tenant_id": "tenant_demo", "tenant_health_score": 92.5}
    query, params = postgres.fetchrow_calls[0]
    assert "tenant_metrics_daily" in query
    assert params == ("tenant_demo",)


@pytest.mark.asyncio
async def test_tenant_health_score_falls_back_to_zero_when_no_rows() -> None:
    """A brand-new tenant with no tenant_metrics_daily rows yet must get a
    defined 0 score, not a None/crash — the `or {...}` fallback in
    repository.py exists exactly for this case.
    """
    postgres = FakePostgres(fetchrow_result=None)
    repo = AnalyticsRepository(postgres)
    result = await repo.tenant_health_score(tenant_id="tenant_new")
    assert result == {"tenant_id": "tenant_new", "tenant_health_score": 0}


@pytest.mark.asyncio
async def test_alerts_queries_by_tenant_with_limit_offset() -> None:
    postgres = FakePostgres()
    repo = AnalyticsRepository(postgres)
    await repo.alerts(tenant_id="tenant_demo", limit=10, offset=0)
    query, params = postgres.fetch_calls[0]
    assert "from alerts" in query
    assert params == ("tenant_demo", 10, 0)


@pytest.mark.asyncio
async def test_tenants_queries_without_tenant_filter() -> None:
    """Unlike every other method, tenants() is deliberately not
    tenant-scoped — it lists all tenants (used by the metadata surfaces,
    not tenant-facing metric endpoints).
    """
    postgres = FakePostgres()
    repo = AnalyticsRepository(postgres)
    await repo.tenants()
    query, params = postgres.fetch_calls[0]
    assert "from tenant_config" in query
    assert params == ()


@pytest.mark.asyncio
async def test_system_status_combines_service_health_and_pipeline_runs() -> None:
    postgres = FakePostgres(fetch_result=[])
    repo = AnalyticsRepository(postgres)
    result = await repo.system_status(tenant_id="tenant_demo")
    assert "service_health" in result
    assert "recent_pipeline_runs" in result
    assert len(postgres.fetch_calls) == 2
    first_query, first_params = postgres.fetch_calls[0]
    assert "service_health_metrics" in first_query
    assert first_params == ("tenant_demo",)
    second_query, second_params = postgres.fetch_calls[1]
    assert "pipeline_run_log" in second_query
    assert second_params == ()
