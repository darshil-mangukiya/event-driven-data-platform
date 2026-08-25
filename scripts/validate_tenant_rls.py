from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

EXPECTED_RLS_TABLES = {
    "event_inbox",
    "event_outbox",
    "privacy_erasure_requests",
    "raw_events",
    "processed_orders",
    "processed_payments",
    "processed_user_sessions",
    "tenant_metrics_hourly",
    "tenant_metrics_daily",
    "alerts",
    "service_health_metrics",
}


def validate_rls_sql(sql: str) -> list[str]:
    errors: list[str] = []
    if "create or replace function current_app_tenant_id" not in sql:
        errors.append("missing current_app_tenant_id helper function")

    for table in sorted(EXPECTED_RLS_TABLES):
        if not re.search(rf"alter table\s+{table}\s+enable row level security", sql, re.IGNORECASE):
            errors.append(f"missing enable row level security for {table}")
        # Runtime verification found that `enable row
        # level security` alone does not protect a table against its own
        # owner's connection — `force row level security` is required
        # too, or RLS is structurally inert for the actual application
        # role that created these tables. See tenant_rls.sql's own header
        # comment for the full live-verified trace.
        if not re.search(rf"alter table\s+{table}\s+force row level security", sql, re.IGNORECASE):
            errors.append(f"missing force row level security for {table} (owner-bypass gap — see tenant_rls.sql header)")
        if not re.search(rf"create policy\s+\w+\s+on\s+{table}", sql, re.IGNORECASE):
            errors.append(f"missing tenant policy for {table}")
        table_block_match = re.search(
            rf"create policy\s+\w+\s+on\s+{table}\s+(.*?);",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )
        table_block = table_block_match.group(1) if table_block_match else ""
        if "tenant_id = current_app_tenant_id()" not in table_block:
            errors.append(f"policy for {table} is not tenant scoped")
        if "with check" not in table_block.lower():
            errors.append(f"policy for {table} is missing WITH CHECK protection")
    return errors


def validation_queries(tenant_id: str) -> list[str]:
    return [
        "begin;",
        f"select set_config('app.tenant_id', '{tenant_id}', true);",
        "select tenant_id, count(*) from tenant_metrics_daily group by tenant_id order by tenant_id;",
        "select tenant_id, count(*) from raw_events group by tenant_id order by tenant_id;",
        "rollback;",
    ]


def run_live_rls_matrix(tenant_scoped_url: str, admin_url: str, tenant_a: str, tenant_b: str) -> dict[str, object]:
    """Actually connects as the dedicated non-superuser
    `platform_tenant_scoped` role (see tenant_rls.sql's header comment for
    why this specific role, not the default `platform` superuser
    connection, is required for RLS to mean anything) and runs the real
    test matrix: own-tenant visibility, cross-tenant denial for two
    distinct tenants, fail-closed with no tenant context set, a rejected
    cross-tenant INSERT, and admin-role full visibility.
    """
    import psycopg2

    results: dict[str, object] = {}

    def _scoped(tenant_id: str | None) -> psycopg2.extensions.connection:
        conn = psycopg2.connect(tenant_scoped_url)
        conn.autocommit = False
        with conn.cursor() as cur:
            if tenant_id is not None:
                cur.execute("select set_config('app.tenant_id', %s, true)", (tenant_id,))
        return conn

    with _scoped(tenant_a) as conn, conn.cursor() as cur:
        cur.execute("select count(*) from processed_orders")
        own_count = cur.fetchone()[0]
        cur.execute("select count(*) from processed_orders where tenant_id = %s", (tenant_b,))
        cross_count = cur.fetchone()[0]
        results["tenant_a_sees_only_own_rows"] = own_count > 0
        results["tenant_a_denied_tenant_b_rows"] = cross_count == 0
        conn.rollback()

    with _scoped(tenant_b) as conn, conn.cursor() as cur:
        cur.execute("select count(*) from processed_orders where tenant_id = %s", (tenant_a,))
        cross_count = cur.fetchone()[0]
        results["tenant_b_denied_tenant_a_rows"] = cross_count == 0
        conn.rollback()

    with _scoped(None) as conn, conn.cursor() as cur:
        cur.execute("select count(*) from processed_orders")
        no_context_count = cur.fetchone()[0]
        results["fails_closed_with_no_tenant_context"] = no_context_count == 0
        conn.rollback()

    with _scoped(tenant_a) as conn, conn.cursor() as cur:
        rejected = False
        try:
            cur.execute(
                "insert into processed_orders (event_id, tenant_id, order_id, customer_id, product_id, "
                "quantity, unit_price, discount_amount, gross_revenue, net_revenue, currency, status, "
                "channel, event_timestamp) values (%s, %s, 'rls-test-order', 'c1', 'p1', 1, 10, 0, 10, 10, "
                "'USD', 'created', 'web', now())",
                (f"rls-live-check-{tenant_b}", tenant_b),
            )
        except Exception:
            rejected = True
        finally:
            conn.rollback()
        results["cross_tenant_insert_rejected"] = rejected

    admin_conn = psycopg2.connect(admin_url)
    try:
        with admin_conn.cursor() as cur:
            cur.execute("select count(distinct tenant_id) from processed_orders")
            distinct_tenants_visible = cur.fetchone()[0]
            results["admin_role_sees_multiple_tenants"] = distinct_tenants_visible >= 2
    finally:
        admin_conn.rollback()
        admin_conn.close()

    results["status"] = "passed" if all(v is True for k, v in results.items() if k != "status") else "failed"
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate tenant RLS policy coverage.")
    parser.add_argument("--sql", default="database/security/tenant_rls.sql")
    parser.add_argument("--tenant-id", default="tenant_demo")
    parser.add_argument("--emit-validation-sql", action="store_true")
    parser.add_argument("--live", action="store_true", help="Run the real runtime test matrix against a live database.")
    parser.add_argument("--tenant-scoped-url", default="postgresql://platform_tenant_scoped:local-tenant-scoped-change-me@localhost:15432/data_platform")
    parser.add_argument("--admin-url", default="postgresql://platform_admin_bypass:local-admin-bypass-change-me@localhost:15432/data_platform")
    parser.add_argument("--tenant-a", default="tenant_demo")
    parser.add_argument("--tenant-b", default="tenant_enterprise")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    sql_path = Path(args.sql)
    errors = validate_rls_sql(sql_path.read_text())
    if errors:
        print("\n".join(errors), file=sys.stderr)
        raise SystemExit(1)

    payload: dict[str, object] = {
        "status": "ok",
        "tables": sorted(EXPECTED_RLS_TABLES),
        "validation_sql": validation_queries(args.tenant_id) if args.emit_validation_sql else [],
    }

    if args.live:
        live_results = run_live_rls_matrix(args.tenant_scoped_url, args.admin_url, args.tenant_a, args.tenant_b)
        payload["live_verification"] = live_results
        if live_results["status"] != "passed":
            payload["status"] = "failed"

    print(json.dumps(payload, indent=2 if args.pretty else None, sort_keys=True))
    if payload["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
