from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

from platform_shared.config import service_settings, validate_settings
from platform_shared.database import Postgres

from data_products.generator import write_reports as write_data_product_reports
from data_products.registry import products_by_id, requirements_by_id
from data_products.validator import validate_all as validate_data_products
from lineage.generator import write_lineage_report
from lineage.graph import build_edges
from lineage.graph import validate_graph as validate_lineage_graph
from platform_cli.tenant_onboarding import (
    TenantOnboardingRequest,
    apply_onboarding,
    build_onboarding_plan,
    readiness_check_sql,
    validate_tenant_readiness,
    write_sample_events,
)
from reliability.runner import available_scenarios, run_all_scenarios, run_scenario
from scripts.backfill_metrics import BackfillRequest, build_backfill_plan, run_backfill
from scripts.generate_evidence_bundle import generate_bundle
from scripts.platform_preflight import run_preflight
from scripts.reconcile_metrics import (
    ALL_CHECK_NAMES,
    CHECK_RUNNERS,
    ReconciliationRequest,
)
from scripts.reconcile_metrics import (
    run_all_checks as run_all_reconciliation_checks,
)
from scripts.reconciliation_summary import run_summary

DEFAULT_DATABASE_URL = "postgresql://platform:platform@localhost:5432/data_platform"


def print_json(payload: dict[str, Any], *, pretty: bool) -> None:
    print(json.dumps(payload, default=str, indent=2 if pretty else None, sort_keys=True))


def database_url(args: argparse.Namespace) -> str:
    return args.database_url or os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)


async def tenant_create(args: argparse.Namespace) -> dict[str, Any]:
    request = TenantOnboardingRequest(
        tenant_id=args.tenant_id,
        tenant_name=args.tenant_name,
        plan=args.plan,
        region=args.region,
        requested_by=args.requested_by,
        sample_event_count=args.sample_events,
        token_ttl_seconds=args.token_ttl_seconds,
        write_sample_events_to=Path(args.output_events) if args.output_events else None,
    )
    if args.dry_run:
        plan = build_onboarding_plan(request)
        if request.write_sample_events_to:
            write_sample_events(request.write_sample_events_to, plan["sample_events"])
        return plan

    postgres = Postgres(database_url(args))
    try:
        return await apply_onboarding(postgres, request)
    finally:
        await postgres.close()


async def tenant_validate(args: argparse.Namespace) -> dict[str, Any]:
    if args.dry_run:
        return {
            "status": "dry_run",
            "tenant_id": args.tenant_id,
            "readiness_checks": readiness_check_sql(args.tenant_id),
        }
    postgres = Postgres(database_url(args))
    try:
        return await validate_tenant_readiness(postgres, args.tenant_id)
    finally:
        await postgres.close()


async def backfill_metrics(args: argparse.Namespace) -> dict[str, Any]:
    request = BackfillRequest(
        tenant_id=args.tenant_id,
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
        requested_by=args.requested_by,
        dry_run=args.dry_run,
        allow_large_window=args.allow_large_window,
    )
    if args.dry_run:
        return {"status": "dry_run", **build_backfill_plan(request)}
    postgres = Postgres(database_url(args))
    try:
        return await run_backfill(postgres, request)
    finally:
        await postgres.close()


async def reconciliation(args: argparse.Namespace) -> dict[str, Any]:
    return await run_summary(
        database_url(args),
        days=args.days,
        tenant_id=args.tenant_id,
        dry_run=args.dry_run,
    )


async def reconciliation_run(args: argparse.Namespace) -> dict[str, Any]:
    """Actually execute a reconciliation check (`ops reconciliation` above
    only summarizes past reconciliation_audit rows — it never runs a fresh
    check). Previously the only way to run one for real was invoking
    `scripts/reconcile_metrics.py` directly; wired into platform_cli here
    for the same reason `data-products` and `lineage` are — one operator
    entrypoint for every governance workflow.
    """
    request = ReconciliationRequest(
        tenant_id=args.tenant_id,
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
        revenue_tolerance=args.revenue_tolerance,
        requested_by=args.requested_by,
        dry_run=args.dry_run,
    )
    if args.check == "all":
        return await run_all_reconciliation_checks(database_url(args), request)
    return await CHECK_RUNNERS[args.check](database_url(args), request)


async def watermarks(args: argparse.Namespace) -> dict[str, Any]:
    query = """
    select pipeline_name, tenant_id, source_topic, last_processed_timestamp,
           last_processed_offset, status, updated_at, metadata
    from pipeline_watermarks
    where ($1::text is null or tenant_id = $1)
    order by updated_at desc, pipeline_name, tenant_id
    limit $2
    """
    if args.dry_run:
        return {"status": "dry_run", "sql": query.strip(), "params": [args.tenant_id, args.limit]}
    postgres = Postgres(database_url(args))
    try:
        rows = await postgres.fetch(query, args.tenant_id, args.limit)
        return {"status": "passed", "count": len(rows), "watermarks": rows}
    finally:
        await postgres.close()


def replay_dlq(args: argparse.Namespace) -> dict[str, Any]:
    command = [
        sys.executable,
        "scripts/dlq_tool.py",
        "--max-records",
        str(args.max_records),
        "replay",
        "--reason",
        args.reason,
        "--replayed-by",
        args.replayed_by,
    ]
    if args.event_id:
        command[2:2] = ["--event-id", args.event_id]
    if args.database_url:
        command.extend(["--database-url", args.database_url])
    if args.dry_run:
        command.append("--dry-run")
        return {"status": "dry_run", "delegated_command": command}
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    return {
        "status": "passed" if completed.returncode == 0 else "failed",
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "delegated_command": command,
    }


def health_check(args: argparse.Namespace) -> dict[str, Any]:
    return run_preflight(dry_run=args.dry_run)


def evidence_generate(args: argparse.Namespace) -> dict[str, Any]:
    bundle = generate_bundle(Path(args.output_dir))
    return {"status": "generated", "output_dir": args.output_dir, **bundle}


def reliability_run(args: argparse.Namespace) -> dict[str, Any]:
    if args.all:
        return run_all_scenarios(artifacts_root=Path(args.artifacts_dir))
    if not args.scenario_id:
        return {"status": "failed", "error": "--scenario-id is required unless --all is passed", "available_scenarios": available_scenarios()}
    return run_scenario(args.scenario_id, artifacts_root=Path(args.artifacts_dir))


def reliability_list(_args: argparse.Namespace) -> dict[str, Any]:
    return {"status": "passed", "scenarios": available_scenarios()}


def config_validate(args: argparse.Namespace) -> dict[str, Any]:
    settings = service_settings(args.service_name)
    errors = validate_settings(settings)
    redacted = {
        "environment": settings.environment,
        "service_name": settings.service_name,
        "database_url": settings.database_url.replace("platform:platform@", "***:***@"),
        "redis_url": settings.redis_url,
        "kafka_bootstrap_servers": settings.kafka_bootstrap_servers,
        "kafka_enable_consumer": settings.kafka_enable_consumer,
        "default_cache_ttl_seconds": settings.default_cache_ttl_seconds,
        "rate_limit_requests_per_minute": settings.rate_limit_requests_per_minute,
    }
    return {"status": "failed" if errors else "passed", "errors": errors, "settings": redacted}


def data_products_list(_args: argparse.Namespace) -> dict[str, Any]:
    products = products_by_id()
    return {
        "status": "passed",
        "count": len(products),
        "products": [
            {
                "product_id": p["product_id"],
                "name": p["name"],
                "domain": p["domain"],
                "modeled_consumers": p.get("modeled_consumers", []),
                "api_endpoint": p.get("api_endpoint"),
                "status": p.get("status"),
            }
            for p in products.values()
        ],
    }


def data_products_show(args: argparse.Namespace) -> dict[str, Any]:
    products = products_by_id()
    product = products.get(args.product_id)
    if product is None:
        return {"status": "failed", "error": f"unknown product_id {args.product_id!r}", "available": sorted(products)}
    return {"status": "passed", "product": product}


def data_products_validate(args: argparse.Namespace) -> dict[str, Any]:
    results = validate_data_products(
        check_live_routes=not args.no_live_routes,
        check_live_tests=not args.no_live_tests,
    )
    total_errors = sum(len(v) for v in results.values())
    return {"status": "failed" if total_errors else "passed", "total_errors": total_errors, "results": results}


def data_products_trace(args: argparse.Namespace) -> dict[str, Any]:
    requirements = requirements_by_id()
    requirement = requirements.get(args.requirement_id)
    if requirement is None:
        return {
            "status": "failed",
            "error": f"unknown requirement_id {args.requirement_id!r}",
            "available": sorted(requirements),
        }
    products = products_by_id()
    product = products.get(requirement["product_id"])
    return {
        "status": "passed",
        "requirement": requirement,
        "product": product,
    }


def data_products_generate(_args: argparse.Namespace) -> dict[str, Any]:
    paths = write_data_product_reports()
    return {"status": "generated", **{k: str(v) for k, v in paths.items()}}


def lineage_validate(_args: argparse.Namespace) -> dict[str, Any]:
    results = validate_lineage_graph()
    total_errors = sum(len(v) for v in results.values())
    return {"status": "failed" if total_errors else "passed", "total_issues": total_errors, "results": results}


def lineage_show(args: argparse.Namespace) -> dict[str, Any]:
    edges = build_edges()
    upstream = sorted({src for src, dst in edges if dst == args.table})
    downstream = sorted({dst for src, dst in edges if src == args.table})
    if not upstream and not downstream:
        return {"status": "failed", "error": f"unknown or disconnected table {args.table!r}"}
    return {"status": "passed", "table": args.table, "upstream": upstream, "downstream": downstream}


def lineage_generate(_args: argparse.Namespace) -> dict[str, Any]:
    path = write_lineage_report()
    return {"status": "generated", "report": str(path)}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Event-Driven Data Platform operator CLI.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    subparsers = parser.add_subparsers(dest="resource", required=True)

    tenant = subparsers.add_parser("tenant", help="Tenant onboarding and validation workflows.")
    tenant_sub = tenant.add_subparsers(dest="action", required=True)
    create = tenant_sub.add_parser("create")
    create.add_argument("--tenant-id", required=True)
    create.add_argument("--tenant-name", required=True)
    create.add_argument("--plan", default="growth")
    create.add_argument("--region", default="us")
    create.add_argument("--requested-by", default=os.getenv("USER", "local-operator"))
    create.add_argument("--sample-events", type=int, default=5)
    create.add_argument("--token-ttl-seconds", type=int, default=86_400)
    create.add_argument("--output-events", default=None)
    create.add_argument("--database-url", default=None)
    create.add_argument("--dry-run", action="store_true")
    create.set_defaults(handler=tenant_create)

    validate = tenant_sub.add_parser("validate")
    validate.add_argument("--tenant-id", required=True)
    validate.add_argument("--database-url", default=None)
    validate.add_argument("--dry-run", action="store_true")
    validate.set_defaults(handler=tenant_validate)

    replay = subparsers.add_parser("replay", help="Replay workflows.")
    replay_sub = replay.add_subparsers(dest="action", required=True)
    dlq = replay_sub.add_parser("dlq")
    dlq.add_argument("--event-id", default=None)
    dlq.add_argument("--max-records", type=int, default=10)
    dlq.add_argument("--reason", default="operator replay from platform_cli")
    dlq.add_argument("--replayed-by", default=os.getenv("USER", "local-operator"))
    dlq.add_argument("--database-url", default=None)
    dlq.add_argument("--dry-run", action="store_true")
    dlq.set_defaults(handler=replay_dlq)

    backfill = subparsers.add_parser("backfill", help="Backfill workflows.")
    backfill_sub = backfill.add_subparsers(dest="action", required=True)
    metrics = backfill_sub.add_parser("metrics")
    metrics.add_argument("--tenant-id", required=True)
    metrics.add_argument("--start-date", required=True)
    metrics.add_argument("--end-date", required=True)
    metrics.add_argument("--requested-by", default=os.getenv("USER", "local-operator"))
    metrics.add_argument("--database-url", default=None)
    metrics.add_argument("--allow-large-window", action="store_true")
    metrics.add_argument("--dry-run", action="store_true")
    metrics.set_defaults(handler=backfill_metrics)

    health = subparsers.add_parser("health", help="Platform health and release checks.")
    health_sub = health.add_subparsers(dest="action", required=True)
    check = health_sub.add_parser("check")
    check.add_argument("--dry-run", action="store_true")
    check.set_defaults(handler=health_check)

    evidence = subparsers.add_parser("evidence", help="Evidence bundle workflows.")
    evidence_sub = evidence.add_subparsers(dest="action", required=True)
    generate = evidence_sub.add_parser("generate")
    generate.add_argument("--output-dir", default="evidence/validation")
    generate.set_defaults(handler=evidence_generate)

    config = subparsers.add_parser("config", help="Configuration validation.")
    config_sub = config.add_subparsers(dest="action", required=True)
    config_check = config_sub.add_parser("validate")
    config_check.add_argument("--service-name", default="platform-cli")
    config_check.set_defaults(handler=config_validate)

    ops = subparsers.add_parser("ops", help="Operational status commands.")
    ops_sub = ops.add_subparsers(dest="action", required=True)
    wm = ops_sub.add_parser("watermarks")
    wm.add_argument("--tenant-id", default=None)
    wm.add_argument("--limit", type=int, default=25)
    wm.add_argument("--database-url", default=None)
    wm.add_argument("--dry-run", action="store_true")
    wm.set_defaults(handler=watermarks)

    recon = ops_sub.add_parser("reconciliation")
    recon.add_argument("--tenant-id", default=None)
    recon.add_argument("--days", type=int, default=7)
    recon.add_argument("--database-url", default=None)
    recon.add_argument("--dry-run", action="store_true")
    recon.set_defaults(handler=reconciliation)

    recon_run = ops_sub.add_parser(
        "reconciliation-run",
        help="Run a reconciliation check (see `ops reconciliation` for prior results).",
    )
    recon_run.add_argument("--tenant-id", required=True)
    recon_run.add_argument("--start-date", required=True)
    recon_run.add_argument("--end-date", required=True)
    recon_run.add_argument("--check", choices=[*ALL_CHECK_NAMES, "all"], default="revenue")
    recon_run.add_argument("--revenue-tolerance", type=float, default=0.01)
    recon_run.add_argument("--requested-by", default=os.getenv("USER", "local-operator"))
    recon_run.add_argument("--database-url", default=None)
    recon_run.add_argument("--dry-run", action="store_true")
    recon_run.set_defaults(handler=reconciliation_run)

    reliability = subparsers.add_parser("reliability", help="Local reliability exercises (failure simulations).")
    reliability_sub = reliability.add_subparsers(dest="action", required=True)

    reliability_list_parser = reliability_sub.add_parser("list", help="List available scenario ids.")
    reliability_list_parser.set_defaults(handler=reliability_list)

    reliability_run_parser = reliability_sub.add_parser("run", help="Run one (or all) reliability scenarios.")
    reliability_run_parser.add_argument("scenario_id", nargs="?", default=None, help="e.g. poison-event, db-outage, redis-outage")
    reliability_run_parser.add_argument("--all", action="store_true", help="Run every registered scenario.")
    reliability_run_parser.add_argument("--artifacts-dir", default="artifacts/reliability")
    reliability_run_parser.set_defaults(handler=reliability_run)

    data_products = subparsers.add_parser("data-products", help="Data product registry, requirements traceability, and validation.")
    data_products_sub = data_products.add_subparsers(dest="action", required=True)

    dp_list_parser = data_products_sub.add_parser("list", help="List registered data products.")
    dp_list_parser.set_defaults(handler=data_products_list)

    dp_show_parser = data_products_sub.add_parser("show", help="Show one data product's full contract.")
    dp_show_parser.add_argument("product_id", help="e.g. revenue, payment_health, customer_activity")
    dp_show_parser.set_defaults(handler=data_products_show)

    dp_validate_parser = data_products_sub.add_parser("validate", help="Cross-reference validate the registry, consumers, and requirements.")
    dp_validate_parser.add_argument("--no-live-routes", action="store_true", help="Skip importing analytics-service to check live API routes.")
    dp_validate_parser.add_argument("--no-live-tests", action="store_true", help="Skip collecting referenced pytest node ids.")
    dp_validate_parser.set_defaults(handler=data_products_validate)

    dp_trace_parser = data_products_sub.add_parser("trace", help="Show the full trace for one requirement id.")
    dp_trace_parser.add_argument("requirement_id", help="e.g. FIN-001, PROD-001, MKT-001, OPS-001, RISK-001")
    dp_trace_parser.set_defaults(handler=data_products_trace)

    dp_generate_parser = data_products_sub.add_parser("generate", help="Generate the data-product catalog and traceability reports.")
    dp_generate_parser.set_defaults(handler=data_products_generate)

    lineage = subparsers.add_parser("lineage", help="Data lineage graph validation and reporting.")
    lineage_sub = lineage.add_subparsers(dest="action", required=True)

    lineage_validate_parser = lineage_sub.add_parser("validate", help="Cycle/orphan detection and cross-reference validation of catalog lineage edges.")
    lineage_validate_parser.set_defaults(handler=lineage_validate)

    lineage_show_parser = lineage_sub.add_parser("show", help="Show upstream/downstream nodes for one table.")
    lineage_show_parser.add_argument("table", help="e.g. tenant_metrics_daily, stream_window_metrics")
    lineage_show_parser.set_defaults(handler=lineage_show)

    lineage_generate_parser = lineage_sub.add_parser("generate", help="Generate the lineage graph report.")
    lineage_generate_parser.set_defaults(handler=lineage_generate)

    return parser


async def run_async(args: argparse.Namespace) -> dict[str, Any]:
    result = args.handler(args)
    if asyncio.iscoroutine(result):
        return await result
    return result


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = asyncio.run(run_async(args))
    print_json(result, pretty=args.pretty)
    if result.get("status") == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
