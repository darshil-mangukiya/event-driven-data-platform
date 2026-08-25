from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from platform_shared.auth import (
    ANALYST_ROLE,
    SERVICE_ACCOUNT_ROLE,
    TENANT_ADMIN_ROLE,
    VIEWER_ROLE,
    TenantPrincipal,
    create_access_token,
)
from platform_shared.database import Postgres
from platform_shared.schemas import EventType, build_envelope, idempotent_event_id


@dataclass(frozen=True)
class TenantUserSpec:
    user_id: str
    email: str
    role: str
    scopes: tuple[str, ...]


@dataclass(frozen=True)
class TenantOnboardingRequest:
    tenant_id: str
    tenant_name: str
    plan: str = "growth"
    region: str = "us"
    requested_by: str = "local-operator"
    sample_event_count: int = 5
    token_ttl_seconds: int = 86_400
    write_sample_events_to: Path | None = None


def default_users(tenant_id: str) -> list[TenantUserSpec]:
    return [
        TenantUserSpec(
            user_id=f"{tenant_id}_admin",
            email=f"admin@{tenant_id}.example.com",
            role=TENANT_ADMIN_ROLE,
            scopes=("metrics:read", "tenant:write", "events:write"),
        ),
        TenantUserSpec(
            user_id=f"{tenant_id}_analyst",
            email=f"analyst@{tenant_id}.example.com",
            role=ANALYST_ROLE,
            scopes=("metrics:read",),
        ),
        TenantUserSpec(
            user_id=f"{tenant_id}_viewer",
            email=f"viewer@{tenant_id}.example.com",
            role=VIEWER_ROLE,
            scopes=("metrics:read",),
        ),
        TenantUserSpec(
            user_id=f"{tenant_id}_svc_ingestion",
            email=f"svc-ingestion@{tenant_id}.example.com",
            role=SERVICE_ACCOUNT_ROLE,
            scopes=("events:write", "metrics:read"),
        ),
    ]


def _stable_event_id(tenant_id: str, event_type: EventType, business_key: str) -> str:
    return idempotent_event_id(
        tenant_id=tenant_id,
        event_type=event_type,
        source_service="tenant-onboarding",
        idempotency_key=business_key,
    )


def sample_events(tenant_id: str, count: int = 5) -> list[dict[str, Any]]:
    event_timestamp = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
    correlation_id = f"onboard-{tenant_id}-0001"
    definitions: list[tuple[EventType, str, dict[str, Any], str | None]] = [
        (
            EventType.ORDER_CREATED,
            "ord_seed_0001:created",
            {
                "order_id": "ord_seed_0001",
                "customer_id": "cust_seed_0001",
                "product_id": "prod_seed_0001",
                "quantity": 2,
                "unit_price": 125.0,
                "discount_amount": 10.0,
                "currency": "USD",
                "status": "created",
                "channel": "web",
                "marketing_campaign_id": "tenant-onboarding",
                "region": "na",
            },
            None,
        ),
        (
            EventType.PAYMENT_CAPTURED,
            "pay_seed_0001:captured",
            {
                "payment_id": "pay_seed_0001",
                "order_id": "ord_seed_0001",
                "customer_id": "cust_seed_0001",
                "amount": 240.0,
                "currency": "USD",
                "status": "captured",
                "payment_method": "card",
                "risk_score": 0.08,
            },
            "ord_seed_0001:created",
        ),
        (
            EventType.USER_ACTIVITY,
            "cust_seed_0001:activity",
            {
                "user_id": "cust_seed_0001",
                "action": "checkout_started",
                "session_id": "sess_seed_0001",
                "page": "/checkout",
                "referrer": "tenant-onboarding",
                "duration_seconds": 180,
                "plan": "growth",
                "marketing_campaign_id": "tenant-onboarding",
            },
            None,
        ),
        (
            EventType.PRODUCT_INVENTORY_CHANGED,
            "prod_seed_0001:inventory_changed",
            {
                "product_id": "prod_seed_0001",
                "sku": "PROD_SEED_0001",
                "name": "Onboarding Seed Product",
                "category": "growth",
                "price": 125.0,
                "inventory_delta": 20,
                "active": True,
            },
            None,
        ),
        (
            EventType.SYSTEM_HEALTH,
            "metadata-service:health",
            {
                "service_name": "metadata-service",
                "status": "healthy",
                "latency_ms": 35.0,
                "error_count": 0,
                "throughput_per_minute": 600.0,
                "kafka_lag": 0,
                "cache_hit_rate": 0.9,
                "message": "tenant onboarding readiness heartbeat",
            },
            None,
        ),
    ]
    events = []
    event_ids_by_key: dict[str, str] = {}
    for index, (event_type, key, payload, causation_key) in enumerate(definitions[:count], start=1):
        event_id = _stable_event_id(tenant_id, event_type, key)
        event_ids_by_key[key] = event_id
        envelope = build_envelope(
            tenant_id=tenant_id,
            event_type=event_type,
            payload=payload,
            source_service="tenant-onboarding",
            event_id=event_id,
            event_timestamp=event_timestamp,
            trace_id=f"onboard-trace-{tenant_id}-{index:04d}",
            correlation_id=correlation_id,
            causation_id=event_ids_by_key.get(causation_key or ""),
            idempotency_key=f"tenant-onboarding:{key}",
            schema_ref="contracts/schemas/v1/event-envelope.schema.json",
        )
        events.append(envelope.model_dump(mode="json"))
    return events


def build_onboarding_plan(request: TenantOnboardingRequest) -> dict[str, Any]:
    users = default_users(request.tenant_id)
    service_account = next(user for user in users if user.role == SERVICE_ACCOUNT_ROLE)
    token = create_access_token(
        TenantPrincipal(
            user_id=service_account.user_id,
            tenant_id=request.tenant_id,
            role=service_account.role,
            scopes=service_account.scopes,
        ),
        expires_in_seconds=request.token_ttl_seconds,
    )
    events = sample_events(request.tenant_id, request.sample_event_count)
    return {
        "status": "planned",
        "tenant": {
            "tenant_id": request.tenant_id,
            "tenant_name": request.tenant_name,
            "plan": request.plan,
            "region": request.region,
            "requested_by": request.requested_by,
        },
        "users": [asdict(user) for user in users],
        "sample_events": events,
        "local_service_account_token": token,
        "readiness_checks": readiness_check_sql(request.tenant_id),
    }


def readiness_check_sql(tenant_id: str) -> list[dict[str, Any]]:
    return [
        {
            "name": "tenant_config_exists",
            "sql": "select count(*) as row_count from tenant_config where tenant_id = $1 and is_active = true",
            "params": [tenant_id],
        },
        {
            "name": "tenant_users_seeded",
            "sql": "select count(*) as row_count from tenant_users where tenant_id = $1 and is_active = true",
            "params": [tenant_id],
        },
        {
            "name": "serving_metrics_seeded",
            "sql": "select count(*) as row_count from tenant_metrics_daily where tenant_id = $1",
            "params": [tenant_id],
        },
        {
            "name": "raw_events_traceable",
            "sql": "select count(*) as row_count from raw_events where tenant_id = $1 and correlation_id is not null",
            "params": [tenant_id],
        },
    ]


async def apply_onboarding(postgres: Postgres, request: TenantOnboardingRequest) -> dict[str, Any]:
    plan = build_onboarding_plan(request)
    await postgres.execute(
        """
        insert into tenant_config (tenant_id, tenant_name, plan, region, is_active, config, updated_at)
        values ($1,$2,$3,$4,true,$5::jsonb,now())
        on conflict (tenant_id) do update set
            tenant_name = excluded.tenant_name,
            plan = excluded.plan,
            region = excluded.region,
            is_active = true,
            config = tenant_config.config || excluded.config,
            updated_at = now()
        """,
        request.tenant_id,
        request.tenant_name,
        request.plan,
        request.region,
        json.dumps({"onboarded_by": request.requested_by, "onboarding_mode": "platform_cli"}),
    )

    for user in default_users(request.tenant_id):
        await postgres.execute(
            """
            insert into tenant_users (tenant_id, user_id, email, role, is_active)
            values ($1,$2,$3,$4,true)
            on conflict (tenant_id, user_id) do update set
                email = excluded.email,
                role = excluded.role,
                is_active = true
            """,
            request.tenant_id,
            user.user_id,
            user.email,
            user.role,
        )

    await postgres.execute(
        """
        insert into tenant_products (
            tenant_id, product_id, sku, name, category, price, inventory_on_hand, active, last_event_id, updated_at
        )
        values ($1,'prod_seed_0001','PROD_SEED_0001','Onboarding Seed Product','growth',125.00,20,true,'tenant-onboarding',now())
        on conflict (tenant_id, product_id) do update set
            inventory_on_hand = excluded.inventory_on_hand,
            active = true,
            updated_at = now()
        """,
        request.tenant_id,
    )

    for event in plan["sample_events"]:
        await postgres.execute(
            """
            insert into raw_events (
                event_id, tenant_id, event_type, event_timestamp, source_service,
                payload_version, payload, trace_id, correlation_id, causation_id,
                idempotency_key, ingested_at
            )
            values ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,now())
            on conflict (event_id) do nothing
            """,
            event["event_id"],
            event["tenant_id"],
            event["event_type"],
            datetime.fromisoformat(event["event_timestamp"]),
            event["source_service"],
            event["payload_version"],
            json.dumps(event["payload"]),
            event["trace_id"],
            event["correlation_id"],
            event["causation_id"],
            event["idempotency_key"],
        )

    await postgres.execute(
        """
        insert into tenant_metrics_daily (
            tenant_id, metric_date, gross_revenue, net_revenue, order_count, units_sold,
            new_users, active_users, payment_success_count, payment_failure_count,
            marketing_spend, marketing_attributed_revenue, events_processed, updated_at
        )
        values ($1,$2,250.00,240.00,1,2,0,1,1,0,3.50,240.00,$3,now())
        on conflict (tenant_id, metric_date) do update set
            gross_revenue = excluded.gross_revenue,
            net_revenue = excluded.net_revenue,
            order_count = excluded.order_count,
            units_sold = excluded.units_sold,
            active_users = excluded.active_users,
            payment_success_count = excluded.payment_success_count,
            events_processed = excluded.events_processed,
            updated_at = now()
        """,
        request.tenant_id,
        date.today(),
        len(plan["sample_events"]),
    )

    if request.write_sample_events_to:
        write_sample_events(request.write_sample_events_to, plan["sample_events"])
    validation = await validate_tenant_readiness(postgres, request.tenant_id)
    return {**plan, "status": "applied", "validation": validation}


async def validate_tenant_readiness(postgres: Postgres, tenant_id: str) -> dict[str, Any]:
    results = []
    for check in readiness_check_sql(tenant_id):
        row = await postgres.fetchrow(check["sql"], *check["params"])
        row_count = int(row["row_count"] if row else 0)
        results.append({**check, "row_count": row_count, "status": "passed" if row_count > 0 else "failed"})
    failed = [result for result in results if result["status"] == "failed"]
    return {"status": "failed" if failed else "passed", "checks": results}


def write_sample_events(path: Path, events: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(event, sort_keys=True) for event in events) + "\n")
