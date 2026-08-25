from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def validate_metric_contracts(contract: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    seen_metrics: set[str] = set()
    if contract.get("contract_version", 0) < 1:
        errors.append("contract_version must be >= 1")
    for metric_contract in contract.get("contracts", []):
        metric = metric_contract.get("metric")
        if not metric:
            errors.append("metric contract missing metric name")
            continue
        if metric in seen_metrics:
            errors.append(f"duplicate metric contract: {metric}")
        seen_metrics.add(metric)
        if not metric_contract.get("endpoint", "").startswith("/metrics/"):
            errors.append(f"{metric} endpoint must be under /metrics")
        required_fields = metric_contract.get("required_fields", [])
        if not required_fields:
            errors.append(f"{metric} missing required_fields")
        if metric_contract.get("tenant_scoped") is not True:
            errors.append(f"{metric} must be tenant_scoped")
        unknown_non_negative = set(metric_contract.get("non_negative_fields", [])) - set(required_fields)
        if unknown_non_negative:
            errors.append(f"{metric} non_negative_fields not in required_fields: {sorted(unknown_non_negative)}")
    return errors


def validate_metric_fixture(metric_contract: dict[str, Any], fixture: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    rows = fixture.get("data", [])
    if not rows and "tenant_health_score" in fixture:
        rows = [fixture]
    for row in rows:
        missing = set(metric_contract["required_fields"]) - set(row)
        if missing:
            errors.append(f"{metric_contract['metric']} fixture missing {sorted(missing)}")
        for field in metric_contract.get("non_negative_fields", []):
            if field in row and float(row[field]) < 0:
                errors.append(f"{metric_contract['metric']} fixture field {field} is negative")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate metric contracts and optional fixtures.")
    parser.add_argument("--contract", default="metrics/contracts/tenant_daily_metrics.json")
    parser.add_argument("--fixture", default=None)
    parser.add_argument("--metric", default=None)
    args = parser.parse_args()
    contract = json.loads(Path(args.contract).read_text())
    errors = validate_metric_contracts(contract)
    if args.fixture and args.metric:
        fixture = json.loads(Path(args.fixture).read_text())
        metric_contracts = {item["metric"]: item for item in contract["contracts"]}
        errors.extend(validate_metric_fixture(metric_contracts[args.metric], fixture))
    if errors:
        print("\n".join(errors), file=sys.stderr)
        raise SystemExit(1)
    print("metric contracts ok")


if __name__ == "__main__":
    main()
