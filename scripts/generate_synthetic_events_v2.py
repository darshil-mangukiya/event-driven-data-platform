from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from platform_shared.schemas import EventType, build_envelope


@dataclass(frozen=True)
class TenantBehavior:
    tenant_id: str
    region: str
    base_order_rate: float
    payment_failure_rate: float
    churn_signal_rate: float
    average_order_value: float
    campaign_mix: tuple[str | None, ...]
    product_categories: tuple[str, ...]


TENANT_BEHAVIORS = [
    TenantBehavior(
        tenant_id="tenant_demo",
        region="na",
        base_order_rate=0.48,
        payment_failure_rate=0.035,
        churn_signal_rate=0.025,
        average_order_value=92.0,
        campaign_mix=("paid-search", "lifecycle", "affiliate", None),
        product_categories=("core", "growth", "marketplace"),
    ),
    TenantBehavior(
        tenant_id="tenant_enterprise",
        region="na",
        base_order_rate=0.22,
        payment_failure_rate=0.018,
        churn_signal_rate=0.012,
        average_order_value=310.0,
        campaign_mix=("enterprise-abm", "field-event", None),
        product_categories=("subscription", "enterprise", "services"),
    ),
    TenantBehavior(
        tenant_id="tenant_marketplace",
        region="eu",
        base_order_rate=0.62,
        payment_failure_rate=0.055,
        churn_signal_rate=0.04,
        average_order_value=64.0,
        campaign_mix=("seller-boost", "paid-social", "affiliate", None),
        product_categories=("marketplace", "seller-tools", "ads"),
    ),
]


def seasonal_multiplier(timestamp: datetime) -> float:
    weekday_boost = 1.15 if timestamp.weekday() in {1, 2, 3} else 0.9
    hour_boost = 1.25 if 14 <= timestamp.hour <= 21 else 0.75
    return weekday_boost * hour_boost


def choose_event_type(behavior: TenantBehavior, timestamp: datetime) -> EventType:
    order_weight = behavior.base_order_rate * seasonal_multiplier(timestamp)
    payment_weight = 0.28
    user_weight = 0.18
    product_weight = 0.06
    system_weight = 0.03
    churn_weight = behavior.churn_signal_rate
    total = order_weight + payment_weight + user_weight + product_weight + system_weight + churn_weight
    draw = random.random() * total
    if draw < order_weight:
        return EventType.ORDER_CREATED
    draw -= order_weight
    if draw < payment_weight:
        return EventType.PAYMENT_FAILED if random.random() < behavior.payment_failure_rate else EventType.PAYMENT_CAPTURED
    draw -= payment_weight
    if draw < user_weight:
        return EventType.USER_ACTIVITY
    draw -= user_weight
    if draw < product_weight:
        return EventType.PRODUCT_INVENTORY_CHANGED
    draw -= product_weight
    if draw < churn_weight:
        return EventType.USER_CHURN_SIGNAL
    return EventType.SYSTEM_HEALTH


def event_payload(behavior: TenantBehavior, event_type: EventType, timestamp: datetime) -> dict:
    customer_id = f"cust_{behavior.tenant_id}_{random.randint(1, 25000):06d}"
    product_id = f"prod_{random.randint(1, 700):04d}"
    order_id = random_id("ord")
    campaign = random.choice(behavior.campaign_mix)
    price = round(max(5.0, random.gauss(behavior.average_order_value, behavior.average_order_value * 0.25)), 2)

    if event_type == EventType.ORDER_CREATED:
        return {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": random.randint(1, 4),
            "unit_price": price,
            "discount_amount": random.choice([0, 0, 0, 5, 10, 25]),
            "currency": "USD" if behavior.region != "eu" else "GBP",
            "status": "created",
            "channel": random.choice(["web", "mobile", "partner", "sales-assisted"]),
            "marketing_campaign_id": campaign,
            "region": behavior.region,
        }

    if event_type in {EventType.PAYMENT_CAPTURED, EventType.PAYMENT_FAILED}:
        risk_score = random.uniform(0.02, 0.65)
        failure_code = None
        status = "captured"
        if event_type == EventType.PAYMENT_FAILED:
            risk_score = random.uniform(0.45, 0.99)
            failure_code = random.choice(["insufficient_funds", "processor_timeout", "risk_block"])
            status = "failed"
        return {
            "payment_id": random_id("pay"),
            "order_id": order_id,
            "customer_id": customer_id,
            "amount": price,
            "currency": "USD" if behavior.region != "eu" else "GBP",
            "status": status,
            "payment_method": random.choice(["card", "wallet", "ach"]),
            "failure_code": failure_code,
            "risk_score": round(risk_score, 3),
        }

    if event_type in {EventType.USER_ACTIVITY, EventType.USER_CHURN_SIGNAL}:
        action = "churn_signal" if event_type == EventType.USER_CHURN_SIGNAL else random.choice(
            ["page_view", "checkout_started", "cart_updated", "feature_used", "support_viewed"]
        )
        return {
            "user_id": customer_id,
            "action": action,
            "session_id": f"sess_{random.randint(1, 900000):08d}",
            "page": random.choice(["/pricing", "/checkout", "/dashboard", "/products", "/billing"]),
            "referrer": random.choice(["organic", "paid", "email", "direct"]),
            "duration_seconds": random.randint(2, 900),
            "plan": random.choice(["starter", "growth", "enterprise"]),
            "marketing_campaign_id": campaign,
        }

    if event_type == EventType.PRODUCT_INVENTORY_CHANGED:
        category = random.choice(behavior.product_categories)
        return {
            "product_id": product_id,
            "sku": product_id.upper(),
            "name": f"{category.title()} Product {product_id[-4:]}",
            "category": category,
            "price": price,
            "inventory_delta": random.randint(-30, 75),
            "active": random.random() > 0.02,
        }

    return {
        "service_name": random.choice(["ingestion-service", "processing-service", "analytics-service"]),
        "status": "degraded" if random.random() < 0.06 else "healthy",
        "latency_ms": round(random.uniform(12, 420), 2),
        "error_count": random.randint(0, 8),
        "throughput_per_minute": round(random.uniform(800, 12000), 2),
        "kafka_lag": random.randint(0, 600),
        "cache_hit_rate": round(random.uniform(0.55, 0.99), 3),
    }


def random_id(prefix: str) -> str:
    return f"{prefix}_{random.getrandbits(56):014x}"


def generate_events(count: int, seed: int | None = None) -> list[str]:
    if seed is not None:
        random.seed(seed)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc) if seed is not None else datetime.now(timezone.utc)
    events: list[str] = []
    for index in range(count):
        behavior = random.choice(TENANT_BEHAVIORS)
        event_timestamp = now - timedelta(minutes=random.randint(0, 7 * 24 * 60))
        event_type = choose_event_type(behavior, event_timestamp)
        envelope = build_envelope(
            tenant_id=behavior.tenant_id,
            event_type=event_type,
            payload=event_payload(behavior, event_type, event_timestamp),
            source_service="synthetic-generator-v2",
            event_timestamp=event_timestamp,
            trace_id=f"synthetic-{index:08d}",
        )
        if seed is not None:
            envelope = envelope.model_copy(
                update={
                    "event_id": f"synthetic-event-{index:08d}",
                    "idempotency_key": f"synthetic-idempotency-{index:08d}",
                }
            )
        events.append(envelope.model_dump_json())
    return events


def post_events(events: list[str], base_url: str, delay_ms: int) -> None:
    grouped: dict[str, list[dict]] = {}
    for raw in events:
        event = json.loads(raw)
        grouped.setdefault(event["tenant_id"], []).append(
            {
                "tenant_id": event["tenant_id"],
                "event_type": event["event_type"],
                "event_timestamp": event["event_timestamp"],
                "payload_version": event["payload_version"],
                "source_service": event["source_service"],
                "payload": event["payload"],
            }
        )

    with httpx.Client(timeout=30) as client:
        for tenant_id, tenant_events in grouped.items():
            for start in range(0, len(tenant_events), 250):
                batch = tenant_events[start : start + 250]
                response = client.post(
                    f"{base_url}/events/batch",
                    headers={"X-Tenant-ID": tenant_id},
                    json={"events": batch},
                )
                response.raise_for_status()
                if delay_ms:
                    time.sleep(delay_ms / 1000)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic tenant-specific local events.")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="data/synthetic/events_v2.jsonl")
    parser.add_argument("--post-to-ingestion", action="store_true")
    parser.add_argument("--ingestion-url", default="http://localhost:8001")
    parser.add_argument("--delay-ms", type=int, default=0)
    args = parser.parse_args()

    events = generate_events(args.count, args.seed)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(events) + "\n")
    if args.post_to_ingestion:
        post_events(events, args.ingestion_url, args.delay_ms)
    print(json.dumps({"events": len(events), "output": str(output_path), "posted": args.post_to_ingestion}))


if __name__ == "__main__":
    main()
