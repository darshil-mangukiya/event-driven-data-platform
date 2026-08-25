from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from platform_shared.database import Postgres


def order_event(tenant_id: str) -> dict:
    event_id = f"evt_load_{random.getrandbits(128):032x}"
    return {
        "event_id": event_id,
        "idempotency_key": event_id,
        "tenant_id": tenant_id,
        "event_type": "order.created",
        "source_service": "load-test",
        "payload": {
            "order_id": f"ord_{random.getrandbits(128):032x}",
            "customer_id": f"cust_{random.randint(1, 10000):05d}",
            "product_id": f"prod_{random.randint(1, 500):04d}",
            "quantity": random.randint(1, 5),
            "unit_price": round(random.uniform(12, 650), 2),
            "discount_amount": random.choice([0, 0, 5, 10, 25]),
            "status": "created",
            "channel": random.choice(["web", "mobile", "partner"]),
            "marketing_campaign_id": random.choice(["paid-search", "lifecycle", "affiliate", None]),
            "region": random.choice(["na", "emea", "apac"]),
        },
    }


async def send_batch(client: httpx.AsyncClient, base_url: str, tenant_id: str, batch_size: int) -> tuple[int, float]:
    started = time.perf_counter()
    response = await client.post(
        f"{base_url}/events/batch",
        headers={"X-Tenant-ID": tenant_id},
        json={"events": [order_event(tenant_id) for _ in range(batch_size)]},
    )
    latency = (time.perf_counter() - started) * 1000
    return response.status_code, latency


async def run(args: argparse.Namespace) -> None:
    random.seed(args.seed)
    latencies: list[float] = []
    failures = 0
    started = time.perf_counter()
    tenant_ids = [item.strip() for item in args.tenant_ids.split(",") if item.strip()]
    if not tenant_ids:
        tenant_ids = [args.tenant_id]
    hot_batches = round(args.batches * args.hot_tenant_share) if len(tenant_ids) > 1 else args.batches
    batch_tenants = [
        tenant_ids[0] if index < hot_batches else tenant_ids[1 + ((index - hot_batches) % (len(tenant_ids) - 1))]
        for index in range(args.batches)
    ]
    async with httpx.AsyncClient(timeout=30) as client:
        tasks = [
            send_batch(client, args.base_url, tenant_id, args.batch_size)
            for tenant_id in batch_tenants
        ]
        for task in asyncio.as_completed(tasks):
            status, latency = await task
            latencies.append(latency)
            if status >= 300:
                failures += 1
    elapsed = time.perf_counter() - started
    total_events = args.batches * args.batch_size
    result = benchmark_result(
        benchmark_name=args.benchmark_name,
        tenant_id=",".join(tenant_ids),
        target_url=args.base_url,
        total_events=total_events,
        elapsed_seconds=elapsed,
        failure_batches=failures,
        latencies=latencies,
        batches=args.batches,
        batch_size=args.batch_size,
        seed=args.seed,
    )
    result["tenant_batch_distribution"] = {
        tenant_id: batch_tenants.count(tenant_id) for tenant_id in tenant_ids
    }
    result["hot_tenant_share"] = args.hot_tenant_share
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2, sort_keys=True))
    if args.database_url:
        await persist_benchmark(args.database_url, result)
    print(json.dumps(result, sort_keys=True))


def percentile(values: list[float], percentile_value: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    index = min(len(values) - 1, max(0, round((percentile_value / 100) * (len(values) - 1))))
    return values[index]


def benchmark_result(
    *,
    benchmark_name: str,
    tenant_id: str,
    target_url: str,
    total_events: int,
    elapsed_seconds: float,
    failure_batches: int,
    latencies: list[float],
    batches: int,
    batch_size: int,
    seed: int | None = None,
) -> dict[str, Any]:
    return {
        "benchmark_name": benchmark_name,
        "tenant_id": tenant_id,
        "target_url": target_url,
        "events": total_events,
        "batches": batches,
        "batch_size": batch_size,
        "seed": seed,
        "elapsed_seconds": round(elapsed_seconds, 4),
        "events_per_second": round(total_events / elapsed_seconds, 4) if elapsed_seconds else 0,
        "failure_batches": failure_batches,
        "failure_rate": round(failure_batches / batches, 4) if batches else 0,
        "p50_latency_ms": round(percentile(latencies, 50), 4) if latencies else None,
        "p95_latency_ms": round(percentile(latencies, 95), 4) if latencies else None,
        "p99_latency_ms": round(percentile(latencies, 99), 4) if latencies else None,
        "max_latency_ms": round(max(latencies), 4) if latencies else None,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "local_scope_note": "Local benchmark runner; production-scale figures require distributed load generation.",
        # Informational only — never used to adjust the pass/fail gate in
        # compare_benchmarks.py. Its purpose is to make it obvious, right
        # in the comparison output, when a "failed" throughput comparison
        # might be explained by comparing two different machines rather
        # than an actual code regression (see benchmarks/README.md).
        "host_cpu_count": os.cpu_count(),
        "host_platform": platform.platform(),
    }


async def persist_benchmark(database_url: str, result: dict[str, Any]) -> None:
    postgres = Postgres(database_url)
    await postgres.execute(
        """
        insert into benchmark_run_results (
            benchmark_name, tenant_id, target_url, total_events, elapsed_seconds,
            events_per_second, failure_count, p50_latency_ms, p95_latency_ms,
            p99_latency_ms, max_latency_ms, result_payload
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
        """,
        result["benchmark_name"],
        result["tenant_id"],
        result["target_url"],
        result["events"],
        result["elapsed_seconds"],
        result["events_per_second"],
        result["failure_batches"],
        result["p50_latency_ms"],
        result["p95_latency_ms"],
        result["p99_latency_ms"],
        result["max_latency_ms"],
        json.dumps(result),
    )
    await postgres.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8001")
    parser.add_argument("--tenant-id", default="tenant_demo")
    parser.add_argument(
        "--tenant-ids",
        default="",
        help="Comma-separated tenant IDs; first tenant receives --hot-tenant-share of batches.",
    )
    parser.add_argument("--hot-tenant-share", type=float, default=1.0)
    parser.add_argument("--batches", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--benchmark-name", default="local_ingestion_batch_load")
    parser.add_argument("--seed", type=int, default=62020)
    parser.add_argument("--output", default=None)
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    args = parser.parse_args()
    if not 0 <= args.hot_tenant_share <= 1:
        parser.error("--hot-tenant-share must be between 0 and 1")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
