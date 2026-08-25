from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_policies(path: Path) -> list[dict[str, Any]]:
    catalog = json.loads(path.read_text())
    policies: list[dict[str, Any]] = []
    for field in catalog["fields"]:
        policy = field["retention_policy"]
        if policy.endswith("_days"):
            days_text = policy.removesuffix("_days")
            if days_text.isdigit():
                policies.append(
                    {
                        "dataset": field["dataset"],
                        "retention_days": int(days_text),
                        "classification": field["classification"],
                        "action": "delete" if field["classification"] != "restricted_pii" else "mask_or_hash",
                    }
                )
    return policies


def retention_sql(table_name: str, retention_days: int) -> str:
    timestamp_column = {
        "api_usage_log": "requested_at",
        "service_health_metrics": "created_at",
        "raw_events": "ingested_at",
        "data_quality_check_results": "checked_at",
    }.get(table_name, "created_at")
    return f"delete from {table_name} where {timestamp_column} < now() - interval '{retention_days} days';"


def build_plan(policies: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "status": "dry_run",
        "policy_count": len(policies),
        "steps": [
            {
                "dataset": policy["dataset"],
                "classification": policy["classification"],
                "retention_days": policy["retention_days"],
                "action": policy["action"],
                "sql": retention_sql(policy["dataset"], policy["retention_days"]),
            }
            for policy in policies
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a dry-run data lifecycle retention plan.")
    parser.add_argument("--privacy-catalog", default="governance/pii_classification.json")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    plan = build_plan(load_policies(Path(args.privacy_catalog)))
    print(json.dumps(plan, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
