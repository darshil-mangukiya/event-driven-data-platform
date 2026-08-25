from __future__ import annotations

import argparse
import time
from uuid import uuid4

import httpx


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant-id", default="tenant_demo")
    parser.add_argument("--metadata-url", default="http://localhost:8004")
    parser.add_argument("--ingestion-url", default="http://localhost:8001")
    parser.add_argument("--analytics-url", default="http://localhost:8003")
    parser.add_argument("--settle-seconds", type=int, default=5)
    args = parser.parse_args()

    with httpx.Client(timeout=20) as client:
        token_response = client.post(
            f"{args.metadata_url}/auth/token",
            headers={"X-Tenant-ID": args.tenant_id},
            json={
                "tenant_id": args.tenant_id,
                "user_id": "local-e2e-runner",
                "role": "tenant_analyst",
                "scopes": ["metrics:read", "events:write"],
                "expires_in_seconds": 3600,
            },
        )
        token_response.raise_for_status()
        token = token_response.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}", "X-Tenant-ID": args.tenant_id}

        order_id = f"ord_e2e_{uuid4().hex[:10]}"
        ingest_response = client.post(
            f"{args.ingestion_url}/events",
            headers=headers,
            json={
                "tenant_id": args.tenant_id,
                "event_type": "order.created",
                "source_service": "local-e2e",
                "payload": {
                    "order_id": order_id,
                    "customer_id": "cust_e2e",
                    "product_id": "prod_001",
                    "quantity": 2,
                    "unit_price": 49.0,
                    "discount_amount": 3.0,
                    "status": "created",
                    "channel": "web",
                    "marketing_campaign_id": "local-e2e",
                    "region": "na",
                },
            },
        )
        ingest_response.raise_for_status()
        print("ingested", ingest_response.json())

        time.sleep(args.settle_seconds)

        revenue_response = client.get(
            f"{args.analytics_url}/metrics/revenue",
            headers=headers,
            params={"tenant_id": args.tenant_id, "limit": 3},
        )
        revenue_response.raise_for_status()
        print("revenue", revenue_response.json())


if __name__ == "__main__":
    main()

