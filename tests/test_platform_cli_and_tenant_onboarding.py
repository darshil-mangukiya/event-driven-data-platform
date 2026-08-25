from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest
from platform_shared.auth import (
    ANALYST_ROLE,
    SERVICE_ACCOUNT_ROLE,
    TenantPrincipal,
    principal_from_headers,
)
from platform_shared.config import Settings, validate_settings
from platform_shared.schemas import EventType, build_envelope

from platform_cli.__main__ import replay_dlq
from platform_cli.tenant_onboarding import (
    TenantOnboardingRequest,
    build_onboarding_plan,
    readiness_check_sql,
    sample_events,
    write_sample_events,
)
from scripts.validate_event_contracts import validate_domain_event_fixtures

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_tenant_onboarding_plan_generates_users_events_and_token(tmp_path: Path) -> None:
    request = TenantOnboardingRequest(
        tenant_id="tenant_newco",
        tenant_name="NewCo Analytics",
        sample_event_count=5,
        write_sample_events_to=tmp_path / "tenant_newco_events.jsonl",
    )
    plan = build_onboarding_plan(request)
    write_sample_events(request.write_sample_events_to, plan["sample_events"])

    assert plan["tenant"]["tenant_id"] == "tenant_newco"
    assert {user["role"] for user in plan["users"]} >= {ANALYST_ROLE, SERVICE_ACCOUNT_ROLE}
    assert len(plan["sample_events"]) == 5
    assert plan["sample_events"][0]["correlation_id"].startswith("onboard-tenant_newco")
    assert plan["sample_events"][0]["idempotency_key"].startswith("tenant-onboarding:")
    assert plan["local_service_account_token"]
    assert request.write_sample_events_to.exists()


def test_tenant_readiness_sql_is_tenant_scoped() -> None:
    checks = readiness_check_sql("tenant_newco")

    assert {check["name"] for check in checks} >= {"tenant_config_exists", "raw_events_traceable"}
    assert all(check["params"] == ["tenant_newco"] for check in checks)


def test_event_envelope_adds_traceability_defaults() -> None:
    envelope = build_envelope(
        tenant_id="tenant_demo",
        event_type=EventType.ORDER_CREATED,
        source_service="test",
        payload={
            "order_id": "ord_1",
            "customer_id": "cust_1",
            "product_id": "prod_1",
            "quantity": 1,
            "unit_price": 10.0,
        },
    )

    assert envelope.correlation_id == envelope.trace_id
    assert envelope.idempotency_key == envelope.event_id


def test_role_validation_rejects_unknown_roles() -> None:
    with pytest.raises(ValueError):
        TenantPrincipal(user_id="bad", tenant_id="tenant_demo", role="owner")

    with pytest.raises(ValueError):
        principal_from_headers(tenant_id="tenant_demo", user_id="bad", role="owner")


def test_config_validation_catches_invalid_environment_and_urls() -> None:
    settings = Settings(
        environment="prodish",
        log_level="INFO",
        service_name="test",
        database_url="sqlite:///local.db",
        redis_url="http://localhost:6379",
        kafka_bootstrap_servers="",
        kafka_client_id="test",
        kafka_consumer_group="test",
        kafka_enable_consumer=True,
        default_cache_ttl_seconds=0,
        rate_limit_requests_per_minute=0,
    )

    errors = validate_settings(settings)

    assert len(errors) >= 5


def test_domain_event_contract_fixtures_are_validated() -> None:
    errors = validate_domain_event_fixtures(PROJECT_ROOT / "contracts" / "events")

    assert errors == []


def test_cli_dlq_replay_dry_run_delegates_to_existing_tool() -> None:
    payload = replay_dlq(
        Namespace(
            event_id="evt_1",
            max_records=1,
            reason="contract test",
            replayed_by="pytest",
            database_url=None,
            dry_run=True,
        )
    )

    assert payload["status"] == "dry_run"
    assert "scripts/dlq_tool.py" in payload["delegated_command"]


def test_sample_events_are_json_serializable_and_traceable() -> None:
    raw = "\n".join(json.dumps(event) for event in sample_events("tenant_newco", 3))

    assert "correlation_id" in raw
    assert "idempotency_key" in raw
