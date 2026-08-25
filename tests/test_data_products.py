"""Tests for the data-product registry, modeled consumer catalog,
requirements traceability matrix, and their cross-reference validation.

Categories: contract (schema) tests, cross-reference tests, traceability
tests, tenant tests, proxy tests, generation tests.
"""

from __future__ import annotations

import re
from pathlib import Path

from data_products.generator import (
    generate_catalog_markdown,
    generate_traceability_markdown,
    write_reports,
)
from data_products.registry import (
    REQUIREMENT_ID_PATTERN,
    consumers_by_id,
    load_analytics_api_routes,
    load_catalog_table_names,
    load_consumers,
    load_metric_contract_names,
    load_registry,
    load_requirements,
    load_slo_names,
    load_source_event_types,
    products_by_id,
    requirements_by_id,
)
from data_products.validator import (
    validate_all,
    validate_consumers,
    validate_proxy_labeling,
    validate_registry,
    validate_requirements,
    validate_traceability,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Contract (schema) tests
# ---------------------------------------------------------------------------


def test_registry_loads_and_is_non_empty() -> None:
    registry = load_registry()
    assert registry["registry_version"] == 1
    assert len(registry["data_products"]) >= 5


def test_registry_has_unique_product_ids() -> None:
    registry = load_registry()
    ids = [p["product_id"] for p in registry["data_products"]]
    assert len(ids) == len(set(ids))


def test_registry_validates_with_no_errors() -> None:
    errors = validate_registry(check_live_routes=False)
    assert errors == []


def test_every_product_has_valid_status() -> None:
    registry = load_registry()
    for product in registry["data_products"]:
        assert product["status"] in ("active", "deprecated", "planned")


def test_every_product_has_a_version() -> None:
    registry = load_registry()
    for product in registry["data_products"]:
        assert isinstance(product["version"], int)
        assert product["version"] >= 1


def test_every_product_has_modeled_owner() -> None:
    registry = load_registry()
    for product in registry["data_products"]:
        assert product.get("owner"), f"{product['product_id']} missing owner"


def test_consumers_load_and_are_unique() -> None:
    consumers = load_consumers()
    ids = [c["consumer_id"] for c in consumers["consumers"]]
    assert len(ids) == len(set(ids))
    assert len(ids) == 5  # finance, product, marketing, operations, risk


def test_consumers_validate_with_no_errors() -> None:
    assert validate_consumers() == []


def test_expected_five_modeled_consumers_present() -> None:
    consumers = consumers_by_id()
    assert set(consumers) == {"finance", "product", "marketing", "operations", "risk"}


# ---------------------------------------------------------------------------
# Cross-reference tests
# ---------------------------------------------------------------------------


def test_every_product_api_endpoint_exists_in_live_analytics_service() -> None:
    routes = load_analytics_api_routes()
    registry = load_registry()
    for product in registry["data_products"]:
        assert product["api_endpoint"] in routes, (
            f"{product['product_id']}: {product['api_endpoint']} not in live analytics-service routes"
        )


def test_every_product_source_event_is_registered() -> None:
    event_types = load_source_event_types()
    registry = load_registry()
    for product in registry["data_products"]:
        for event_type in product.get("source_events", []):
            assert event_type in event_types, f"{product['product_id']}: unknown source_event {event_type}"


def test_every_product_serving_table_is_cataloged() -> None:
    tables = load_catalog_table_names()
    registry = load_registry()
    for product in registry["data_products"]:
        assert product["serving_table"] in tables, f"{product['product_id']}: uncataloged serving_table"


def test_every_product_metric_contract_exists() -> None:
    metric_names = load_metric_contract_names()
    registry = load_registry()
    for product in registry["data_products"]:
        for metric in product.get("metric_contracts", []):
            assert metric in metric_names, f"{product['product_id']}: unknown metric {metric}"


def test_every_product_slo_reference_exists_in_slo_catalog() -> None:
    slo_names = load_slo_names()
    assert len(slo_names) >= 8
    registry = load_registry()
    for product in registry["data_products"]:
        for slo in product.get("slo_reference", []):
            assert slo in slo_names, f"{product['product_id']}: unknown SLO {slo}"


def test_every_product_dependency_points_to_a_real_product() -> None:
    registry = load_registry()
    product_ids = {p["product_id"] for p in registry["data_products"]}
    for product in registry["data_products"]:
        for dep in product.get("dependencies", []):
            assert dep in product_ids, f"{product['product_id']}: dependency {dep} is not a registered product"


def test_full_cross_reference_validation_passes_live() -> None:
    """The complete cross-reference validation, against live API routes and
    live pytest collection of every test_reference — the strongest form of
    this check, not a superficial schema-only pass.
    """
    results = validate_all(check_live_routes=True, check_live_tests=True)
    total_errors = sum(len(v) for v in results.values())
    assert total_errors == 0, results


# ---------------------------------------------------------------------------
# Traceability tests
# ---------------------------------------------------------------------------


def test_requirements_load_and_ids_are_unique() -> None:
    requirements = load_requirements()
    ids = [r["requirement_id"] for r in requirements["requirements"]]
    assert len(ids) == len(set(ids))
    assert len(ids) >= 8


def test_requirement_ids_follow_naming_convention() -> None:
    requirements = load_requirements()
    for req in requirements["requirements"]:
        assert REQUIREMENT_ID_PATTERN.match(req["requirement_id"]), (
            f"invalid requirement id format: {req['requirement_id']}"
        )


def test_requirements_validate_with_no_errors() -> None:
    errors = validate_requirements(check_live_tests=False)
    assert errors == []


def test_every_active_requirement_traces_end_to_end() -> None:
    """Every requirement must resolve: consumer -> product -> source event ->
    processing -> serving table -> API -> validation -> acceptance criteria.
    """
    errors = validate_traceability()
    assert errors == []


def test_requirement_consumer_is_listed_in_product_modeled_consumers() -> None:
    requirements = requirements_by_id()
    products = products_by_id()
    for req_id, req in requirements.items():
        product = products[req["product_id"]]
        assert req["consumer_id"] in product["modeled_consumers"], (
            f"{req_id}: consumer {req['consumer_id']} not listed under product {req['product_id']}'s modeled_consumers"
        )


def test_at_least_one_requirement_per_consumer() -> None:
    requirements = load_requirements()
    consumer_ids_with_requirements = {r["consumer_id"] for r in requirements["requirements"]}
    assert consumer_ids_with_requirements == {"finance", "product", "marketing", "operations", "risk"}


def test_full_fin_001_trace_matches_expected_chain() -> None:
    """Demonstrates one complete, concrete trace end-to-end (also used as
    the reference example in evidence/data-products/requirements-traceability.md).
    """
    requirements = requirements_by_id()
    req = requirements["FIN-001"]
    assert req["consumer_id"] == "finance"
    assert req["product_id"] == "revenue"
    assert req["metric"] == "revenue"
    assert req["source_event"] == "order.created"
    assert req["serving_table"] == "tenant_metrics_daily"
    assert req["api_endpoint"] == "/metrics/revenue"
    assert req["test_reference"]
    assert len(req["acceptance_criteria"]) >= 1

    products = products_by_id()
    product = products[req["product_id"]]
    assert req["consumer_id"] in product["modeled_consumers"]
    assert product["api_endpoint"] == req["api_endpoint"]
    assert product["serving_table"] == req["serving_table"]


def test_every_requirement_test_reference_is_a_real_collectible_test() -> None:
    """This is the strongest form of the traceability claim: the
    test_reference isn't just a plausible-looking string, it's a pytest
    node id that actually collects.
    """
    import subprocess
    import sys

    requirements = load_requirements()
    node_ids = sorted({r["test_reference"] for r in requirements["requirements"] if r.get("test_reference")})
    assert node_ids

    import os

    env = os.environ.copy()
    env["PYTHONPATH"] = f"{PROJECT_ROOT}:{PROJECT_ROOT / 'services' / 'shared'}"
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", *node_ids],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr


# ---------------------------------------------------------------------------
# Tenant isolation tests
# ---------------------------------------------------------------------------


def test_every_tenant_scoped_product_declares_isolation_rule() -> None:
    registry = load_registry()
    for product in registry["data_products"]:
        isolation = product.get("tenant_isolation")
        assert isolation is not None, f"{product['product_id']} missing tenant_isolation"
        if isolation.get("tenant_scoped"):
            assert isolation.get("isolation_rule"), f"{product['product_id']} tenant_scoped but no isolation_rule"


def test_all_current_products_are_tenant_scoped() -> None:
    """Every current data product in this platform is tenant-scoped — a
    product that claimed tenant_scoped=false would need a stronger
    justification than exists today, so this is a deliberate assertion,
    not an accident of the current registry contents.
    """
    registry = load_registry()
    for product in registry["data_products"]:
        assert product["tenant_isolation"]["tenant_scoped"] is True, (
            f"{product['product_id']} is not tenant scoped — verify this is intentional"
        )


def test_isolation_rule_references_a_real_enforcement_mechanism() -> None:
    """The isolation_rule text should point at an actual mechanism in the
    codebase (assert_tenant_scope, tenant_id filter, etc.), not a vague
    claim.
    """
    registry = load_registry()
    known_mechanisms = ("assert_tenant_scope", "tenant_id", "TenantPrincipal")
    for product in registry["data_products"]:
        rule = product["tenant_isolation"]["isolation_rule"]
        assert any(mechanism in rule for mechanism in known_mechanisms), (
            f"{product['product_id']}: isolation_rule doesn't reference a known enforcement mechanism"
        )


# ---------------------------------------------------------------------------
# Proxy metric honesty tests
# ---------------------------------------------------------------------------


def test_proxy_products_pass_proxy_labeling_validation() -> None:
    assert validate_proxy_labeling() == []


def test_churn_and_retention_are_labeled_as_signal_or_proxy() -> None:
    registry = load_registry()
    products = products_by_id(registry)
    customer_activity = products["customer_activity"]
    limitations_text = " ".join(customer_activity.get("known_limitations", [])).lower()
    assert "signal" in limitations_text or "proxy" in limitations_text
    assert "confirmed churn" not in limitations_text
    assert "cohort-based retention" not in limitations_text or "not a" in limitations_text


def test_marketing_roi_labeled_as_proxy_not_attribution_model() -> None:
    registry = load_registry()
    products = products_by_id(registry)
    marketing = products["marketing_performance"]
    text = (marketing["description"] + " " + " ".join(marketing.get("known_limitations", []))).lower()
    assert "proxy" in text
    # Should explicitly deny being a production attribution model, not claim to be one
    assert re.search(r"not\s+an?\s+production attribution model", text) or "proxy, not a production attribution model" in text


def test_no_metric_contract_forbidden_language() -> None:
    """Sanity check against the underlying metric contract JSON too, not
    just the data-product registry layer.
    """
    import json

    from data_products.registry import METRIC_CONTRACTS_PATH

    contract = json.loads(METRIC_CONTRACTS_PATH.read_text())
    forbidden = ["confirmed churn", "guaranteed retention", "production attribution model"]
    for metric_contract in contract["contracts"]:
        text = json.dumps(metric_contract).lower()
        for phrase in forbidden:
            assert phrase not in text or "not" in text, f"{metric_contract['metric']} may overclaim: {phrase}"


# ---------------------------------------------------------------------------
# Generation tests (deterministic)
# ---------------------------------------------------------------------------


def test_catalog_markdown_generation_is_deterministic() -> None:
    md1 = generate_catalog_markdown()
    md2 = generate_catalog_markdown()
    assert md1 == md2


def test_traceability_markdown_generation_is_deterministic() -> None:
    md1 = generate_traceability_markdown()
    md2 = generate_traceability_markdown()
    assert md1 == md2


def test_catalog_markdown_contains_all_products() -> None:
    registry = load_registry()
    md = generate_catalog_markdown()
    for product in registry["data_products"]:
        assert product["name"] in md
        assert product["api_endpoint"] in md


def test_traceability_markdown_contains_all_requirements() -> None:
    requirements = load_requirements()
    md = generate_traceability_markdown()
    for req in requirements["requirements"]:
        assert req["requirement_id"] in md


def test_write_reports_creates_files(tmp_path: Path) -> None:
    paths = write_reports(output_dir=tmp_path)
    assert paths["catalog"].exists()
    assert paths["traceability"].exists()
    assert paths["catalog"].read_text().startswith("# Data Product Catalog")
    assert paths["traceability"].read_text().startswith("# Requirements Traceability Report")


def test_evidence_reports_are_up_to_date() -> None:
    """The checked-in evidence/data-products/*.md files should match what
    the generator produces right now — catches stale, hand-edited, or
    forgotten-to-regenerate reports.
    """
    evidence_dir = PROJECT_ROOT / "evidence" / "data-products"
    catalog_path = evidence_dir / "data-product-catalog.md"
    traceability_path = evidence_dir / "requirements-traceability.md"
    assert catalog_path.exists(), "run `make data-products-catalog` to generate evidence"
    assert traceability_path.exists(), "run `make requirements-trace` to generate evidence"
    assert catalog_path.read_text() == generate_catalog_markdown()
    assert traceability_path.read_text() == generate_traceability_markdown()


# ---------------------------------------------------------------------------
# CLI integration tests
# ---------------------------------------------------------------------------


def test_cli_data_products_list() -> None:
    from platform_cli.__main__ import data_products_list

    result = data_products_list(None)
    assert result["status"] == "passed"
    assert result["count"] >= 5


def test_cli_data_products_show_known_product() -> None:
    import argparse

    from platform_cli.__main__ import data_products_show

    result = data_products_show(argparse.Namespace(product_id="revenue"))
    assert result["status"] == "passed"
    assert result["product"]["product_id"] == "revenue"


def test_cli_data_products_show_unknown_product() -> None:
    import argparse

    from platform_cli.__main__ import data_products_show

    result = data_products_show(argparse.Namespace(product_id="does-not-exist"))
    assert result["status"] == "failed"


def test_cli_data_products_trace_known_requirement() -> None:
    import argparse

    from platform_cli.__main__ import data_products_trace

    result = data_products_trace(argparse.Namespace(requirement_id="FIN-001"))
    assert result["status"] == "passed"
    assert result["requirement"]["requirement_id"] == "FIN-001"


def test_cli_data_products_trace_unknown_requirement() -> None:
    import argparse

    from platform_cli.__main__ import data_products_trace

    result = data_products_trace(argparse.Namespace(requirement_id="ZZZ-999"))
    assert result["status"] == "failed"


def test_cli_parser_registers_data_products_subcommand() -> None:
    from platform_cli.__main__ import build_parser

    parser = build_parser()
    args = parser.parse_args(["data-products", "list"])
    assert args.resource == "data-products"
    assert args.action == "list"
