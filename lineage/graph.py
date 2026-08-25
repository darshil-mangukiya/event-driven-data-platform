"""Lineage graph built from catalog/data_catalog.json's upstream/downstream
declarations, plus cross-reference validation of the *specific, checkable*
claims in that graph against the actual code that would have to exist for
the claim to be true.

Two kinds of nodes appear in the catalog's upstream/downstream lists:

* **Table nodes** — every entry in ``catalog/data_catalog.json``'s
  ``tables`` array. These are always structurally real (they're the
  catalog itself).
* **External nodes** — dotted-namespace references to something outside
  the catalog: an API (``analytics.metrics_api``), a service
  (``app.ops_console``), a job (``spark.batch_revenue_aggregates``), a dbt
  model (``dbt.fct_tenant_daily_metrics``), a reliability exercise
  (``reliability.late_event_exercise``), Kafka (``kafka.domain_topics``),
  or a purely descriptive reference (``docs.*``, ``platform.*``,
  ``governance.*``, ``slo.*``, ``ops.*``, ``sql.*``, ``system.*``,
  ``source.*``). Only the first group — nodes naming a *specific* file this
  module knows how to check — get cross-referenced against real code;
  the rest are accepted as descriptive/structural without a code check
  (see ``VERIFIABLE_NODE_SOURCES`` and ``GENERIC_NODE_PREFIXES`` below).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_CATALOG_PATH = PROJECT_ROOT / "catalog" / "data_catalog.json"

# Node prefixes that are accepted as descriptive/structural references —
# real concepts, but not a single file this module can grep to confirm a
# specific table name appears. Cycle/orphan detection still covers them;
# only the per-claim code cross-reference is skipped.
GENERIC_NODE_PREFIXES = (
    "docs.",
    "platform.",
    "governance.",
    "slo.",
    "ops.",
    "sql.",
    "system.",
    "source.",
    "kafka.",  # Kafka topics are infrastructure, not a single checkable file
)
GENERIC_NODES = {"dbt.jobs", "spark.jobs"}

# Specific, checkable external nodes -> the file(s) whose content must
# contain the table's name for the claimed edge to be considered verified.
# A dbt mart model additionally resolves one hop through its own
# `ref('<staging_model>')` calls (see _dbt_model_text_including_refs)
# because marts commonly reference a staging model, not the raw table,
# by design (dbt's own layering convention) — that's a legitimate indirect
# reference, not a broken one.
VERIFIABLE_NODE_SOURCES: dict[str, list[Path]] = {
    "analytics.metrics_api": [
        PROJECT_ROOT / "services" / "analytics-service" / "app" / "repository.py",
        PROJECT_ROOT / "services" / "analytics-service" / "app" / "main.py",
    ],
    "analytics.alerts_api": [
        PROJECT_ROOT / "services" / "analytics-service" / "app" / "repository.py",
        PROJECT_ROOT / "services" / "analytics-service" / "app" / "main.py",
    ],
    "analytics.tenant_health_score_api": [
        PROJECT_ROOT / "services" / "analytics-service" / "app" / "repository.py",
        PROJECT_ROOT / "services" / "analytics-service" / "app" / "main.py",
    ],
    "app.ops_console": [
        PROJECT_ROOT / "services" / "ops-console" / "app" / "main.py",
        PROJECT_ROOT / "services" / "ops-console" / "app" / "observability.py",
    ],
    "app.demo_dashboard": [
        PROJECT_ROOT / "services" / "demo-dashboard" / "app" / "main.py",
    ],
    "platform_cli.ops_watermarks": [
        PROJECT_ROOT / "platform_cli" / "__main__.py",
    ],
    "spark.streaming.streaming_job": [
        PROJECT_ROOT / "spark" / "streaming" / "streaming_job.py",
        PROJECT_ROOT / "spark" / "streaming" / "sinks.py",
    ],
    "spark.batch_revenue_aggregates": [
        PROJECT_ROOT / "spark" / "jobs" / "batch_revenue_aggregates.py",
    ],
    "spark.tenant_user_session_summary_stage": [
        PROJECT_ROOT / "spark" / "jobs" / "sessionization_job.py",
    ],
    "dbt.fct_tenant_daily_metrics": [
        PROJECT_ROOT / "dbt" / "models" / "marts" / "fct_tenant_daily_metrics.sql",
    ],
    "dbt.fct_product_performance": [
        PROJECT_ROOT / "dbt" / "models" / "marts" / "fct_product_performance.sql",
    ],
    "reliability.late_event_exercise": [
        PROJECT_ROOT / "reliability" / "scenarios" / "late_event.py",
    ],
    "reliability.incident_artifacts": [
        PROJECT_ROOT / "reliability" / "evidence.py",
        PROJECT_ROOT / "reliability" / "scenarios" / "db_outage.py",
        PROJECT_ROOT / "reliability" / "scenarios" / "poison_event.py",
    ],
    "reliability.runner": [
        PROJECT_ROOT / "reliability" / "runner.py",
    ],
    "services.schema_registry_service": [
        PROJECT_ROOT / "services" / "schema-registry-service" / "app" / "main.py",
        PROJECT_ROOT / "services" / "schema-registry-service" / "app" / "repository.py",
    ],
    "scripts.validate_schema_registry": [
        PROJECT_ROOT / "scripts" / "validate_schema_registry.py",
    ],
    "scripts.backfill_metrics": [
        PROJECT_ROOT / "scripts" / "backfill_metrics.py",
    ],
    "processing-service": [
        PROJECT_ROOT / "services" / "processing-service" / "app" / "repository.py",
    ],
}

_DBT_REF_PATTERN = re.compile(r"ref\(\s*['\"]([a-zA-Z0-9_]+)['\"]\s*\)")


def load_catalog() -> dict[str, Any]:
    return json.loads(DATA_CATALOG_PATH.read_text())


def table_names(catalog: dict[str, Any] | None = None) -> set[str]:
    catalog = catalog or load_catalog()
    return {t["name"] for t in catalog["tables"]}


def build_edges(catalog: dict[str, Any] | None = None) -> list[tuple[str, str]]:
    """Every (from, to) edge implied by upstream/downstream declarations."""
    catalog = catalog or load_catalog()
    edges: list[tuple[str, str]] = []
    for table in catalog["tables"]:
        name = table["name"]
        for upstream in table.get("upstream", []):
            edges.append((upstream, name))
        for downstream in table.get("downstream", []):
            edges.append((name, downstream))
    return edges


def find_cycles(catalog: dict[str, Any] | None = None) -> list[list[str]]:
    """Detect cycles in the full lineage graph (tables + external nodes)
    via DFS. A real lineage DAG should never have a cycle — data doesn't
    flow back into its own upstream source in this platform.
    """
    edges = build_edges(catalog)
    adjacency: dict[str, list[str]] = {}
    for src, dst in edges:
        adjacency.setdefault(src, []).append(dst)

    cycles: list[list[str]] = []
    visited: set[str] = set()
    stack: list[str] = []
    on_stack: set[str] = set()

    def dfs(node: str) -> None:
        visited.add(node)
        stack.append(node)
        on_stack.add(node)
        for neighbor in adjacency.get(node, []):
            if neighbor not in visited:
                dfs(neighbor)
            elif neighbor in on_stack:
                cycle_start = stack.index(neighbor)
                cycles.append(stack[cycle_start:] + [neighbor])
        stack.pop()
        on_stack.discard(node)

    for node in list(adjacency):
        if node not in visited:
            dfs(node)
    return cycles


def find_orphan_tables(catalog: dict[str, Any] | None = None) -> list[str]:
    """Tables with neither upstream nor downstream declared — likely a
    forgotten cataloging gap, since every real table in this platform is
    written by something and read by something.
    """
    catalog = catalog or load_catalog()
    return [
        t["name"]
        for t in catalog["tables"]
        if not t.get("upstream") and not t.get("downstream")
    ]


def _file_or_dbt_refs_contain(table_name: str, path: Path) -> bool:
    if not path.exists():
        return False
    content = path.read_text()
    if table_name in content:
        return True
    if path.suffix == ".sql":
        for ref_model in _DBT_REF_PATTERN.findall(content):
            ref_path = path.parent.parent / "staging" / f"{ref_model}.sql"
            if ref_path.exists() and table_name in ref_path.read_text():
                return True
    return False


def verify_checkable_edges(catalog: dict[str, Any] | None = None) -> list[str]:
    """For every (table, external_node) edge where external_node is a
    *specific, checkable* node (in VERIFIABLE_NODE_SOURCES), confirm the
    table's name actually appears in the mapped source file(s) — a real
    cross-reference against code, in addition to internal consistency between
    two catalog fields.
    """
    catalog = catalog or load_catalog()
    errors: list[str] = []
    for table in catalog["tables"]:
        name = table["name"]
        for node in set(table.get("upstream", [])) | set(table.get("downstream", [])):
            sources = VERIFIABLE_NODE_SOURCES.get(node)
            if sources is None:
                continue
            if not any(_file_or_dbt_refs_contain(name, path) for path in sources):
                errors.append(
                    f"catalog claims an edge between table '{name}' and '{node}', "
                    f"but '{name}' does not appear in any of {[str(p.relative_to(PROJECT_ROOT)) for p in sources]}"
                )
    return errors


def verify_all_external_nodes_recognized(catalog: dict[str, Any] | None = None) -> list[str]:
    """Every external node referenced in the catalog must be either a
    known table, a specifically-checkable node, or a recognized generic
    prefix/node — catches typos and orphaned references to nothing.
    """
    catalog = catalog or load_catalog()
    tables = table_names(catalog)
    errors: list[str] = []
    all_nodes: set[str] = set()
    for table in catalog["tables"]:
        all_nodes.update(table.get("upstream", []))
        all_nodes.update(table.get("downstream", []))
    for node in sorted(all_nodes - tables):
        if node in VERIFIABLE_NODE_SOURCES or node in GENERIC_NODES:
            continue
        if any(node.startswith(prefix) for prefix in GENERIC_NODE_PREFIXES):
            continue
        errors.append(f"unrecognized external lineage node: {node!r} — not a table, not in VERIFIABLE_NODE_SOURCES, no matching generic prefix")
    return errors


def validate_graph(catalog: dict[str, Any] | None = None) -> dict[str, list[str]]:
    catalog = catalog or load_catalog()
    cycles = find_cycles(catalog)
    return {
        "cycles": [" -> ".join(cycle) for cycle in cycles],
        "orphan_tables": find_orphan_tables(catalog),
        "unverified_edges": verify_checkable_edges(catalog),
        "unrecognized_nodes": verify_all_external_nodes_recognized(catalog),
    }
