from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True)
class ConsumerRequest:
    team: str
    endpoint: str
    params: dict[str, Any]
    business_use: str


TEAM_REQUESTS = {
    "product": [
        ConsumerRequest(
            team="product",
            endpoint="/metrics/customers",
            params={"limit": 14},
            business_use="Track active users and user growth by tenant.",
        ),
        ConsumerRequest(
            team="product",
            endpoint="/metrics/product_performance",
            params={"limit": 10},
            business_use="Rank products by revenue and units sold.",
        ),
    ],
    "finance": [
        ConsumerRequest(
            team="finance",
            endpoint="/metrics/revenue",
            params={"limit": 30},
            business_use="Produce daily tenant revenue reporting.",
        ),
        ConsumerRequest(
            team="finance",
            endpoint="/metrics/payment_success",
            params={"limit": 30},
            business_use="Monitor payment success and failure rates.",
        ),
    ],
    "marketing": [
        ConsumerRequest(
            team="marketing",
            endpoint="/metrics/marketing_roi",
            params={"limit": 25},
            business_use="Compare campaign spend to attributed revenue.",
        )
    ],
    "operations": [
        ConsumerRequest(
            team="operations",
            endpoint="/system/status",
            params={},
            business_use="Check platform freshness, service health, and cache status.",
        ),
        ConsumerRequest(
            team="operations",
            endpoint="/alerts",
            params={"limit": 20},
            business_use="Review open operational alerts for a tenant.",
        ),
    ],
    "risk": [
        ConsumerRequest(
            team="risk",
            endpoint="/metrics/payment_success",
            params={"limit": 30},
            business_use="Track failed payments and risk-related operational signals.",
        ),
        ConsumerRequest(
            team="risk",
            endpoint="/metrics/tenant_health_score",
            params={},
            business_use="Prioritize tenant health review when payment failures or churn signals rise.",
        ),
    ],
}


def build_request_plan(base_url: str, tenant_id: str, team: str) -> list[dict[str, Any]]:
    if team not in TEAM_REQUESTS:
        raise ValueError(f"unknown team {team!r}; expected one of {sorted(TEAM_REQUESTS)}")
    plan = []
    for request in TEAM_REQUESTS[team]:
        params = {"tenant_id": tenant_id, **request.params}
        plan.append(
            {
                "team": request.team,
                "business_use": request.business_use,
                "method": "GET",
                "url": f"{base_url.rstrip('/')}{request.endpoint}",
                "headers": {"X-Tenant-ID": tenant_id, "X-User-Role": "analyst"},
                "params": params,
            }
        )
    return plan


def execute_plan(plan: list[dict[str, Any]], token: str | None = None) -> list[dict[str, Any]]:
    responses = []
    with httpx.Client(timeout=15) as client:
        for item in plan:
            headers = dict(item["headers"])
            if token:
                headers["Authorization"] = f"Bearer {token}"
            response = client.get(item["url"], headers=headers, params=item["params"])
            responses.append(
                {
                    "team": item["team"],
                    "url": item["url"],
                    "status_code": response.status_code,
                    "business_use": item["business_use"],
                    "response": response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text,
                }
            )
    return responses


def main() -> None:
    parser = argparse.ArgumentParser(description="Show or run internal analytics API consumer examples.")
    parser.add_argument("--base-url", default="http://localhost:8003")
    parser.add_argument("--tenant-id", default="tenant_demo")
    parser.add_argument("--team", choices=sorted(TEAM_REQUESTS), required=True)
    parser.add_argument("--token", default=None)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    plan = build_request_plan(args.base_url, args.tenant_id, args.team)
    payload = {"mode": "execute" if args.execute else "plan", "requests": execute_plan(plan, args.token) if args.execute else plan}
    print(json.dumps(payload, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
