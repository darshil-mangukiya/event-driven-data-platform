from __future__ import annotations

import argparse

import httpx


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-ingestion", default="http://localhost:8001")
    parser.add_argument("--base-analytics", default="http://localhost:8003")
    parser.add_argument("--tenant-id", default="tenant_demo")
    args = parser.parse_args()

    headers = {"X-Tenant-ID": args.tenant_id}
    with httpx.Client(timeout=10) as client:
        for url in [
            f"{args.base_ingestion}/health",
            f"{args.base_analytics}/health",
            f"{args.base_analytics}/metrics/revenue?tenant_id={args.tenant_id}&limit=2",
            f"{args.base_analytics}/system/status?tenant_id={args.tenant_id}",
        ]:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            print(url, response.status_code)


if __name__ == "__main__":
    main()

