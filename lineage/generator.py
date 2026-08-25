"""Generate a human-readable lineage graph report from
catalog/data_catalog.json, deterministically (same catalog -> byte-identical
output, no timestamps).

Output: evidence/lineage/lineage-graph.md — a Mermaid flowchart per domain
plus the full validation summary (cycles, orphans, unverified/unrecognized
edges), so the graph and its own integrity check live in one generated
artifact.
"""

from __future__ import annotations

from pathlib import Path

from lineage.graph import PROJECT_ROOT, load_catalog, validate_graph

DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "evidence" / "lineage"


def _mermaid_node_id(name: str) -> str:
    return name.replace(".", "_").replace("-", "_")


def generate_mermaid_by_domain(catalog: dict | None = None) -> dict[str, str]:
    """One Mermaid flowchart per domain, showing each domain's tables and
    their immediate upstream/downstream nodes (external nodes included so
    the diagram shows where data actually comes from / goes to, beyond
    table-to-table edges).
    """
    catalog = catalog or load_catalog()
    by_domain: dict[str, list[str]] = {}
    for table in catalog["tables"]:
        domain = table.get("domain", "unassigned")
        lines = by_domain.setdefault(domain, [])
        table_id = _mermaid_node_id(table["name"])
        for upstream in table.get("upstream", []):
            up_id = _mermaid_node_id(upstream)
            lines.append(f'    {up_id}["{upstream}"] --> {table_id}["{table["name"]}"]')
        for downstream in table.get("downstream", []):
            down_id = _mermaid_node_id(downstream)
            lines.append(f'    {table_id}["{table["name"]}"] --> {down_id}["{downstream}"]')
    return {domain: "\n".join(sorted(set(lines))) for domain, lines in by_domain.items()}


def generate_lineage_report_markdown(catalog: dict | None = None) -> str:
    catalog = catalog or load_catalog()
    validation = validate_graph(catalog)
    diagrams = generate_mermaid_by_domain(catalog)

    lines = [
        "# Data Lineage Graph",
        "",
        "Generated from `catalog/data_catalog.json`. Do not hand-edit —",
        "regenerate with `python scripts/generate_lineage_report.py` or",
        "`make lineage-graph`.",
        "",
        "Every edge below either connects two cataloged tables, or connects",
        "a table to an external node (`analytics.*`, `app.*`, `spark.*`,",
        "`dbt.*`, `reliability.*`, `platform_cli.*`) whose claim is",
        "cross-referenced against real code by `lineage/graph.py` — see",
        "\"Graph Validation\" below and `docs/lineage.md` \"What this",
        "framework itself caught\" for the mismatches this check has found.",
        "",
        "## Graph Validation",
        "",
        f"- Cycles detected: **{len(validation['cycles'])}**",
        f"- Orphan tables (no upstream or downstream): **{len(validation['orphan_tables'])}**",
        f"- Unverified edges (claimed but not found in code): **{len(validation['unverified_edges'])}**",
        f"- Unrecognized external nodes: **{len(validation['unrecognized_nodes'])}**",
        "",
    ]
    for category, items in validation.items():
        if items:
            lines.append(f"### {category.replace('_', ' ').title()}")
            lines.append("")
            for item in items:
                lines.append(f"- {item}")
            lines.append("")

    lines.append("## Lineage by Domain")
    lines.append("")
    for domain in sorted(diagrams):
        lines.append(f"### {domain.title()}")
        lines.append("")
        lines.append("```mermaid")
        lines.append("flowchart LR")
        lines.append(diagrams[domain])
        lines.append("```")
        lines.append("")

    lines.append("## Table Reference")
    lines.append("")
    lines.append("| Table | Domain | Layer | Owner | Upstream | Downstream |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for table in sorted(catalog["tables"], key=lambda t: t["name"]):
        upstream = ", ".join(table.get("upstream", [])) or "—"
        downstream = ", ".join(table.get("downstream", [])) or "—"
        lines.append(
            f"| `{table['name']}` | {table.get('domain', '—')} | {table.get('layer', '—')} | "
            f"{table.get('owner', '—')} | {upstream} | {downstream} |"
        )

    return "\n".join(lines) + "\n"


def write_lineage_report(output_dir: Path | None = None) -> Path:
    output_dir = output_dir or DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "lineage-graph.md"
    report_path.write_text(generate_lineage_report_markdown())
    return report_path
