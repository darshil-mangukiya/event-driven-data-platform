"""Generate human-readable reports from the machine-readable data-product
contracts. Deterministic: same input files -> byte-identical output (no
timestamps, no random ordering) so generation is safe to test with a plain
equality assertion.

Outputs:
    evidence/data-products/data-product-catalog.md
    evidence/data-products/requirements-traceability.md
"""

from __future__ import annotations

from pathlib import Path

from data_products.registry import (
    PROJECT_ROOT,
    consumers_by_id,
    load_consumers,
    load_registry,
    load_requirements,
    products_by_id,
)

DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "evidence" / "data-products"


def generate_catalog_markdown(
    registry: dict | None = None,
    consumers: dict | None = None,
) -> str:
    registry = registry or load_registry()
    consumers_map = consumers_by_id(consumers)

    lines = [
        "# Data Product Catalog",
        "",
        "Generated from `contracts/data_products/registry.yml`. Do not hand-edit —",
        "regenerate with `python scripts/generate_data_product_catalog.py` or",
        "`make data-products-catalog`.",
        "",
        "These are modeled internal data products used to document the platform design.",
        "See [docs/consumer-requirements.md](../../docs/consumer-requirements.md)",
        "for the modeled-vs-real distinction.",
        "",
        "| Product | Consumers | Metrics | API | Freshness | SLO | Quality | Tenant Scope | Status |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for product in registry["data_products"]:
        consumer_names = ", ".join(
            consumers_map.get(c, {}).get("name", c) for c in product.get("modeled_consumers", [])
        )
        metrics = ", ".join(product.get("metric_contracts", []))
        quality = ", ".join(product.get("data_quality_rules", [])) or "—"
        slo = "; ".join(product.get("slo_reference", [])) or "—"
        tenant_scope = "Yes" if product.get("tenant_isolation", {}).get("tenant_scoped") else "No"
        lines.append(
            f"| {product['name']} | {consumer_names} | {metrics} | `{product['api_endpoint']}` | "
            f"{product.get('freshness_target', '—')} | {slo} | {quality} | {tenant_scope} | {product['status']} |"
        )

    lines.append("")
    lines.append("## Product Detail")
    lines.append("")
    for product in registry["data_products"]:
        lines.append(f"### {product['name']} (`{product['product_id']}`)")
        lines.append("")
        lines.append(f"- **Domain**: {product['domain']}")
        lines.append(f"- **Owner (modeled)**: {product['owner']}")
        lines.append(f"- **Description**: {product['description'].strip()}")
        lines.append(f"- **Modeled consumers**: {', '.join(product.get('modeled_consumers', []))}")
        lines.append("- **Business questions**:")
        for q in product.get("business_questions", []):
            lines.append(f"  - {q}")
        lines.append(f"- **Source events**: {', '.join(product.get('source_events', []))}")
        lines.append(f"- **Serving table**: `{product['serving_table']}`")
        lines.append(f"- **API endpoint**: `{product['api_endpoint']}`")
        lines.append(f"- **Grain**: {', '.join(product.get('grain', []))}")
        lines.append(f"- **Measures**: {', '.join(product.get('measures', []))}")
        lines.append(f"- **Freshness target**: {product.get('freshness_target', '—')}")
        lines.append(f"- **Latency target**: {product.get('latency_target', '—')}")
        lines.append(f"- **SLO reference**: {', '.join(product.get('slo_reference', []))}")
        isolation = product.get("tenant_isolation", {})
        lines.append(f"- **Tenant scoped**: {isolation.get('tenant_scoped')}")
        lines.append(f"- **Isolation rule**: {isolation.get('isolation_rule', '—').strip()}")
        lines.append("- **Lineage**:")
        for lineage_step in product.get("lineage", []):
            lines.append(f"  - {lineage_step}")
        cache = product.get("cache", {})
        lines.append(
            f"- **Cache**: enabled={cache.get('enabled')}, ttl={cache.get('ttl_seconds')}s, "
            f"fallback: {cache.get('fallback_behavior', '—')}"
        )
        lines.append("- **Failure behavior**:")
        for f in product.get("failure_behavior", []):
            lines.append(f"  - {f}")
        lines.append("- **Acceptance criteria**:")
        for a in product.get("acceptance_criteria", []):
            lines.append(f"  - {a}")
        if product.get("known_limitations"):
            lines.append("- **Known limitations**:")
            for k in product["known_limitations"]:
                lines.append(f"  - {k}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def generate_traceability_markdown(
    requirements: dict | None = None,
    registry: dict | None = None,
    consumers: dict | None = None,
) -> str:
    requirements = requirements or load_requirements()
    products = products_by_id(registry)
    consumers_map = consumers_by_id(consumers)

    lines = [
        "# Requirements Traceability Report",
        "",
        "Generated from `contracts/data_products/requirements.yml`. Do not",
        "hand-edit — regenerate with `python scripts/generate_data_product_catalog.py`",
        "or `make requirements-trace`.",
        "",
        f"Total requirements: {len(requirements['requirements'])}",
        "",
        "| Requirement | Consumer | Business Question | Product | Metric | Source Event | "
        "Serving Table | API | Validation | SLO | Status |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for req in requirements["requirements"]:
        consumer_name = consumers_map.get(req["consumer_id"], {}).get("name", req["consumer_id"])
        product_name = products.get(req["product_id"], {}).get("name", req["product_id"])
        lines.append(
            f"| {req['requirement_id']} | {consumer_name} | {req['business_question']} | {product_name} | "
            f"{req['metric']} | `{req['source_event']}` | `{req['serving_table']}` | `{req['api_endpoint']}` | "
            f"{req['validation_rule']} | {req['slo_reference']} | {req['status']} |"
        )

    lines.append("")
    lines.append("## Full Trace Detail")
    lines.append("")
    for req in requirements["requirements"]:
        lines.append(f"### {req['requirement_id']}: {req['business_question']}")
        lines.append("")
        lines.append("```text")
        lines.append(f"{req['requirement_id']}")
        lines.append(f"  -> Consumer: {consumers_map.get(req['consumer_id'], {}).get('name', req['consumer_id'])}")
        lines.append(f"  -> Business Question: {req['business_question']}")
        lines.append(f"  -> Data Product: {products.get(req['product_id'], {}).get('name', req['product_id'])} ({req['product_id']})")
        lines.append(f"  -> Metric: {req['metric']}")
        lines.append(f"  -> Source Event: {req['source_event']}")
        lines.append(f"  -> Processing: {req['processing_logic']}")
        lines.append(f"  -> Serving Table: {req['serving_table']}")
        lines.append(f"  -> API Endpoint: {req['api_endpoint']}")
        lines.append(f"  -> Validation Rule: {req['validation_rule']}")
        lines.append(f"  -> SLO: {req['slo_reference']}")
        lines.append(f"  -> Test Reference: {req.get('test_reference', '—')}")
        lines.append("```")
        lines.append("")
        lines.append("Acceptance criteria:")
        for a in req["acceptance_criteria"]:
            lines.append(f"- {a}")
        lines.append("")

    return "\n".join(lines) + "\n"


def write_reports(output_dir: Path | None = None) -> dict[str, Path]:
    output_dir = output_dir or DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    registry = load_registry()
    consumers = load_consumers()
    requirements = load_requirements()

    catalog_path = output_dir / "data-product-catalog.md"
    catalog_path.write_text(generate_catalog_markdown(registry, consumers))

    traceability_path = output_dir / "requirements-traceability.md"
    traceability_path.write_text(generate_traceability_markdown(requirements, registry, consumers))

    return {"catalog": catalog_path, "traceability": traceability_path}
