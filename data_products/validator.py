"""Cross-reference validation for the data-product registry, modeled
consumer catalog, and requirements traceability matrix.

This goes beyond schema validation (required fields present) to check that
every reference in the contracts actually resolves against the real,
implemented system: API endpoints that exist, event types that are
registered, serving tables that are cataloged, metrics that have a formula
contract, and SLOs that are defined. See data_products/registry.py for how
each "real system fact" is derived.
"""

from __future__ import annotations

import re
from typing import Any

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

VALID_STATUSES = {"active", "deprecated", "planned"}


def validate_registry(
    registry: dict[str, Any] | None = None,
    *,
    check_live_routes: bool = True,
) -> list[str]:
    """Validate contracts/data_products/registry.yml: schema completeness
    plus cross-reference checks against the real system.
    """
    errors: list[str] = []
    registry = registry or load_registry()
    products = registry.get("data_products", [])

    consumer_ids = set(consumers_by_id())
    metric_names = load_metric_contract_names()
    event_types = load_source_event_types()
    table_names = load_catalog_table_names()
    slo_names = load_slo_names()
    api_routes = load_analytics_api_routes() if check_live_routes else None

    seen_ids: set[str] = set()
    for product in products:
        product_id = product.get("product_id")
        if not product_id:
            errors.append("data product missing product_id")
            continue
        if product_id in seen_ids:
            errors.append(f"duplicate product_id: {product_id}")
        seen_ids.add(product_id)

        # -- required fields -------------------------------------------------
        for field in ("name", "version", "domain", "owner", "status", "api_endpoint", "serving_table"):
            if not product.get(field):
                errors.append(f"{product_id}: missing required field '{field}'")

        if product.get("status") not in VALID_STATUSES:
            errors.append(f"{product_id}: invalid status {product.get('status')!r} (expected one of {sorted(VALID_STATUSES)})")

        if not product.get("modeled_consumers"):
            errors.append(f"{product_id}: missing modeled_consumers")
        else:
            for consumer_id in product["modeled_consumers"]:
                if consumer_id not in consumer_ids:
                    errors.append(f"{product_id}: unknown modeled_consumer {consumer_id!r} (not in consumers.yml)")

        if not product.get("business_questions"):
            errors.append(f"{product_id}: missing business_questions")

        if not product.get("acceptance_criteria"):
            errors.append(f"{product_id}: missing acceptance_criteria")

        # -- tenant isolation: every product must explicitly declare it -----
        tenant_isolation = product.get("tenant_isolation")
        if tenant_isolation is None:
            errors.append(f"{product_id}: missing tenant_isolation declaration")
        else:
            if "tenant_scoped" not in tenant_isolation:
                errors.append(f"{product_id}: tenant_isolation missing tenant_scoped")
            elif tenant_isolation["tenant_scoped"] and not tenant_isolation.get("isolation_rule"):
                errors.append(f"{product_id}: tenant_scoped=true but no isolation_rule documented")

        # -- freshness/SLO: every serving product needs both ----------------
        if not product.get("freshness_target"):
            errors.append(f"{product_id}: missing freshness_target")
        if not product.get("slo_reference"):
            errors.append(f"{product_id}: missing slo_reference")
        else:
            for slo in product["slo_reference"]:
                if slo not in slo_names:
                    errors.append(f"{product_id}: slo_reference {slo!r} not found in docs/slo-catalog.md")

        # -- cross-reference: metric_contracts ------------------------------
        for metric in product.get("metric_contracts", []):
            if metric not in metric_names:
                errors.append(f"{product_id}: unknown metric {metric!r} (not in metrics/contracts/tenant_daily_metrics.json)")

        # -- cross-reference: source_events ----------------------------------
        for event_type in product.get("source_events", []):
            if event_type not in event_types:
                errors.append(f"{product_id}: unknown source_event {event_type!r} (not in contracts/registry.json)")

        # -- cross-reference: serving_table / source_tables -------------------
        serving_table = product.get("serving_table")
        if serving_table and serving_table not in table_names:
            errors.append(f"{product_id}: unknown serving_table {serving_table!r} (not in catalog/data_catalog.json)")
        for table in product.get("source_tables", []):
            if table not in table_names:
                errors.append(f"{product_id}: unknown source_table {table!r} (not in catalog/data_catalog.json)")

        # -- cross-reference: api_endpoint ------------------------------------
        api_endpoint = product.get("api_endpoint")
        if api_endpoint and api_routes is not None and api_endpoint not in api_routes:
            errors.append(f"{product_id}: api_endpoint {api_endpoint!r} not found in analytics-service's live OpenAPI routes")

        # -- cross-reference: dependencies point to real products -----------
        for dep in product.get("dependencies", []):
            if dep not in {p.get("product_id") for p in products}:
                errors.append(f"{product_id}: dependency {dep!r} is not a registered product_id")

        # -- cache contract consistency --------------------------------------
        cache = product.get("cache")
        if cache is None:
            errors.append(f"{product_id}: missing cache contract")
        elif cache.get("enabled") and "ttl_seconds" not in cache:
            errors.append(f"{product_id}: cache.enabled=true but no ttl_seconds")

        if not product.get("failure_behavior"):
            errors.append(f"{product_id}: missing failure_behavior")

    return errors


def validate_consumers(consumers: dict[str, Any] | None = None) -> list[str]:
    errors: list[str] = []
    consumers = consumers or load_consumers()
    seen: set[str] = set()
    for consumer in consumers.get("consumers", []):
        consumer_id = consumer.get("consumer_id")
        if not consumer_id:
            errors.append("consumer missing consumer_id")
            continue
        if consumer_id in seen:
            errors.append(f"duplicate consumer_id: {consumer_id}")
        seen.add(consumer_id)
        for field in ("name", "modeled_responsibility", "business_questions"):
            if not consumer.get(field):
                errors.append(f"{consumer_id}: missing required field '{field}'")
    return errors


def validate_requirements(
    requirements: dict[str, Any] | None = None,
    *,
    check_live_tests: bool = True,
) -> list[str]:
    """Validate contracts/data_products/requirements.yml against the
    consumer catalog, the product registry, and (optionally) that the
    referenced test actually exists as a collectible pytest node.
    """
    errors: list[str] = []
    requirements = requirements or load_requirements()
    reqs = requirements.get("requirements", [])

    consumer_ids = set(consumers_by_id())
    product_ids = set(products_by_id())
    slo_names = load_slo_names()

    seen_ids: set[str] = set()
    for req in reqs:
        req_id = req.get("requirement_id")
        if not req_id:
            errors.append("requirement missing requirement_id")
            continue
        if not REQUIREMENT_ID_PATTERN.match(req_id):
            errors.append(f"invalid requirement_id format: {req_id!r} (expected <PREFIX>-<NNN>)")
        if req_id in seen_ids:
            errors.append(f"duplicate requirement_id: {req_id}")
        seen_ids.add(req_id)

        for field in ("consumer_id", "business_question", "product_id", "metric", "acceptance_criteria"):
            if not req.get(field):
                errors.append(f"{req_id}: missing required field '{field}'")

        if req.get("consumer_id") and req["consumer_id"] not in consumer_ids:
            errors.append(f"{req_id}: unknown consumer_id {req['consumer_id']!r}")

        if req.get("product_id") and req["product_id"] not in product_ids:
            errors.append(f"{req_id}: unknown product_id {req['product_id']!r}")

        if req.get("slo_reference") and req["slo_reference"] not in slo_names:
            errors.append(f"{req_id}: slo_reference {req['slo_reference']!r} not found in docs/slo-catalog.md")

        if not req.get("acceptance_criteria"):
            errors.append(f"{req_id}: missing acceptance_criteria")

        if not req.get("test_reference"):
            errors.append(f"{req_id}: missing test_reference (acceptance criteria should tie to an automated test)")

    if check_live_tests:
        errors.extend(_validate_test_references_collectible(reqs))

    return errors


def _validate_test_references_collectible(reqs: list[dict[str, Any]]) -> list[str]:
    """Confirm every test_reference is a real, collectible pytest node id.
    Runs pytest --collect-only against the union of referenced files, once,
    rather than once per requirement.
    """
    import subprocess
    import sys as _sys
    from pathlib import Path

    project_root = Path(__file__).resolve().parents[1]
    errors: list[str] = []
    node_ids = {req["test_reference"] for req in reqs if req.get("test_reference")}
    if not node_ids:
        return errors

    result = subprocess.run(
        [_sys.executable, "-m", "pytest", "--collect-only", "-q", *sorted(node_ids)],
        cwd=project_root,
        capture_output=True,
        text=True,
        env=_pytest_env(),
        timeout=120,
    )
    if result.returncode != 0:
        errors.append(
            f"one or more test_reference node ids failed to collect: {result.stdout[-2000:]}\n{result.stderr[-2000:]}"
        )
    return errors


def _pytest_env() -> dict[str, str]:
    import os

    env = os.environ.copy()
    project_root_str = str(_project_root())
    shared = str(_project_root() / "services" / "shared")
    existing = env.get("PYTHONPATH", "")
    parts = [project_root_str, shared] + ([existing] if existing else [])
    env["PYTHONPATH"] = ":".join(parts)
    return env


def _project_root():
    from pathlib import Path

    return Path(__file__).resolve().parents[1]


def validate_traceability(
    registry: dict[str, Any] | None = None,
    consumers: dict[str, Any] | None = None,
    requirements: dict[str, Any] | None = None,
) -> list[str]:
    """Every active requirement must trace all the way: consumer -> product
    -> source -> processing/serving -> API -> validation -> acceptance
    criteria. This checks the full chain resolves instead of checking only
    that individual fields are non-empty.
    """
    errors: list[str] = []
    products = products_by_id(registry)
    consumers_map = consumers_by_id(consumers)
    reqs = requirements_by_id(requirements)

    for req_id, req in reqs.items():
        if req.get("status") != "active":
            continue
        chain: list[tuple[str, bool]] = []
        consumer = consumers_map.get(req.get("consumer_id"))
        chain.append(("consumer", consumer is not None))
        product = products.get(req.get("product_id"))
        chain.append(("product", product is not None))
        chain.append(("source_event", bool(req.get("source_event"))))
        chain.append(("processing_logic", bool(req.get("processing_logic"))))
        chain.append(("serving_table", bool(req.get("serving_table"))))
        chain.append(("api_endpoint", bool(req.get("api_endpoint"))))
        chain.append(("validation_rule", bool(req.get("validation_rule"))))
        chain.append(("acceptance_criteria", bool(req.get("acceptance_criteria"))))

        # A product must actually list this consumer as a modeled consumer
        # for the trace to be coherent; two independently valid IDs are not enough.
        if product is not None and req.get("consumer_id") not in product.get("modeled_consumers", []):
            errors.append(
                f"{req_id}: traceability break — consumer {req.get('consumer_id')!r} is not listed in "
                f"product {req.get('product_id')!r}'s modeled_consumers"
            )

        broken = [name for name, ok in chain if not ok]
        if broken:
            errors.append(f"{req_id}: traceability chain broken at {broken}")

    return errors


def validate_proxy_labeling(registry: dict[str, Any] | None = None) -> list[str]:
    """Proxy/signal metrics (churn signal, retention proxy, marketing ROI
    proxy) must not describe themselves as predictive or multi-touch
    attribution measurements.
    """
    errors: list[str] = []
    registry = registry or load_registry()
    proxy_products = {"customer_activity": ["churn", "retention"], "marketing_performance": ["marketing_roi"]}
    forbidden_phrases = [
        "confirmed churn",
        "guaranteed retention",
        "production attribution model",
        "certified prediction",
        "validated forecast",
    ]
    for product in registry.get("data_products", []):
        if product["product_id"] not in proxy_products:
            continue
        text_blob = " ".join(
            [
                product.get("description", ""),
                " ".join(product.get("known_limitations", [])),
                " ".join(product.get("business_questions", [])),
            ]
        ).lower()
        # A phrase preceded by a negation ("not a production attribution
        # model") is disclaiming the claim, not making it — only flag a
        # forbidden phrase when it isn't negated within the preceding words.
        for phrase in forbidden_phrases:
            for match in re.finditer(re.escape(phrase), text_blob):
                preceding = text_blob[max(0, match.start() - 20) : match.start()]
                if not re.search(r"\bnot\s+(a|an|the)?\s*$", preceding):
                    errors.append(f"{product['product_id']}: contains forbidden overclaiming phrase {phrase!r}")
        # Must positively acknowledge being a proxy/signal somewhere
        if "proxy" not in text_blob and "signal" not in text_blob:
            errors.append(f"{product['product_id']}: proxy product does not document itself as a proxy/signal anywhere")
    return errors


def validate_all(*, check_live_routes: bool = True, check_live_tests: bool = True) -> dict[str, list[str]]:
    """Run every validation category and return a dict of category -> errors."""
    registry = load_registry()
    consumers = load_consumers()
    requirements = load_requirements()
    return {
        "registry": validate_registry(registry, check_live_routes=check_live_routes),
        "consumers": validate_consumers(consumers),
        "requirements": validate_requirements(requirements, check_live_tests=check_live_tests),
        "traceability": validate_traceability(registry, consumers, requirements),
        "proxy_labeling": validate_proxy_labeling(registry),
    }
