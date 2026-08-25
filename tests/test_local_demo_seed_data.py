"""Tests for the local demo transactional seed data
(database/init/003_local_demo_transactional_seed.sql).

Previously, database/init/002_seed.sql seeded tenant config and
hand-typed tenant_metrics_daily aggregates but left raw_events,
processed_orders, processed_payments, and processed_user_sessions
completely empty — so a fresh `docker compose up` had every reconciliation
check  fail by default (the "recomputed" side was 0),
and /metrics/product_performance, /metrics/marketing_roi, and
/metrics/event_throughput showed nothing.

Structural tests here run without a live database (parsing the SQL file
directly); the integration-marked tests connect to a live PostgreSQL if
reachable and verify the derived data is actually internally consistent —
see "Verification" in docs/local-data-generation.md for the full live
trace, including a real edge-case bug this identified and corrected (a payment
timestamp that could cross midnight, producing a spurious extra
tenant_metrics_daily row).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from reliability.injectors.reachability import postgres_reachable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SEED_SQL_PATH = PROJECT_ROOT / "database" / "init" / "003_local_demo_transactional_seed.sql"
BASE_SEED_SQL_PATH = PROJECT_ROOT / "database" / "init" / "002_seed.sql"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://platform:platform@localhost:5432/data_platform")


# ---------------------------------------------------------------------------
# Structural tests (no live database required)
# ---------------------------------------------------------------------------


def test_seed_sql_file_exists_and_runs_after_base_seed() -> None:
    """docker-entrypoint-initdb.d runs .sql files in filename order —
    003 must sort after 002 so tenant_config/tenant_products already
    exist (this file's FK references and product_id lookups depend on
    them).
    """
    assert SEED_SQL_PATH.exists()
    assert SEED_SQL_PATH.name > BASE_SEED_SQL_PATH.name


def test_seed_sql_inserts_into_all_four_processed_layer_tables() -> None:
    sql = SEED_SQL_PATH.read_text()
    for table in ("processed_orders", "raw_events", "processed_payments", "processed_user_sessions"):
        assert f"insert into {table}" in sql, f"missing insert into {table}"


def test_seed_sql_derives_tenant_metrics_daily_not_hardcodes_it() -> None:
    """Regression: tenant_metrics_daily must be computed from an
    aggregation over the seeded processed_* tables (a `group by` /
    `sum`/`count` derived INSERT ... SELECT), not a second, independently
    hand-typed literal-values INSERT — that duplication is exactly the
    class of gap validation identified and corrected elsewhere in this
    project.
    """
    sql = SEED_SQL_PATH.read_text()
    tenant_metrics_section = sql[sql.index("insert into tenant_metrics_daily") :]
    assert "from orders_agg" in tenant_metrics_section
    assert "group by" in sql.lower()


def test_base_seed_no_longer_hardcodes_tenant_metrics_daily() -> None:
    """Regression: database/init/002_seed.sql must not reintroduce a
    hand-typed tenant_metrics_daily INSERT — that would silently make it
    a second, drifting source of truth alongside 003's derived values.
    """
    sql = BASE_SEED_SQL_PATH.read_text()
    assert "insert into tenant_metrics_daily" not in sql


def test_seed_sql_payment_timestamp_is_clamped_to_same_day() -> None:
    """Regression for a defect found during runtime verification:
    a naive `event_timestamp + interval '2 minutes'` for a payment's
    timestamp can cross midnight for orders placed in the last two
    minutes of a day, producing a spurious extra tenant_metrics_daily row
    with a mismatched date. Fixed with a `least(...)` clamp to the same
    calendar day.
    """
    sql = SEED_SQL_PATH.read_text()
    assert "least(event_timestamp + interval '2 minutes'" in sql
    assert "date_trunc('day', event_timestamp)" in sql


def test_seed_sql_covers_all_three_seed_tenants() -> None:
    sql = SEED_SQL_PATH.read_text()
    for tenant_id in ("tenant_demo", "tenant_enterprise", "tenant_marketplace"):
        assert tenant_id in sql


def test_seed_sql_alerts_capped_per_tenant_not_globally() -> None:
    """Regression: a single global LIMIT on the alerts insert let one
    high-failure-rate tenant crowd out the others — fixed with a
    per-tenant row_number() cap so every tenant is represented in the
    demo's Incidents view.
    """
    sql = SEED_SQL_PATH.read_text()
    assert "row_number() over (partition by tenant_id" in sql
    assert "where rn <=" in sql


def test_seed_sql_only_touches_seed_prefixed_rows() -> None:
    """The seed data uses a `seed-order-`/`seed-payment-`/`seed-session-`
    event_id prefix consistently, so later aggregation steps (and any
    future cleanup) can distinguish seed rows from real data written by
    an actual running pipeline without ambiguity.
    """
    sql = SEED_SQL_PATH.read_text()
    assert "'seed-order-%'" in sql
    assert "'seed-payment-%'" in sql
    assert "'seed-session-%'" in sql


# ---------------------------------------------------------------------------
# Live verification (integration-marked; skips cleanly if no live DB)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_live_seed_data_produces_internally_consistent_tenant_metrics_daily() -> None:
    if not postgres_reachable(DATABASE_URL):
        pytest.skip(f"PostgreSQL not reachable at {DATABASE_URL}")

    import psycopg2
    import psycopg2.extras

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                select tenant_id, metric_date, order_count, payment_success_count,
                       payment_failure_count, active_users
                from tenant_metrics_daily
                order by tenant_id, metric_date
                """
            )
            rows = cur.fetchall()
            if not rows:
                pytest.skip("tenant_metrics_daily is empty — seed data not loaded in this database")

            for row in rows:
                cur.execute(
                    "select count(*) as n from processed_orders where tenant_id = %s and event_timestamp::date = %s",
                    (row["tenant_id"], row["metric_date"]),
                )
                real_order_count = cur.fetchone()["n"]
                assert real_order_count == row["order_count"], (
                    f"{row['tenant_id']}/{row['metric_date']}: tenant_metrics_daily.order_count "
                    f"({row['order_count']}) does not match a fresh count over processed_orders "
                    f"({real_order_count}) — the derived seed data has drifted from its source"
                )
    finally:
        conn.close()
