"""Loaders for the data-product contracts and the real-system facts they're
cross-referenced against.

Every ``load_*_from_system`` function reads the *actual implemented system*
(not a hand-maintained mirror list) so a broken reference is a genuine
detection, not a comparison against another hardcoded guess:

* API routes    -> imports the real FastAPI app and reads ``app.openapi()``
* Source events -> parses ``contracts/registry.json`` (the event contract registry)
* Serving tables -> parses ``catalog/data_catalog.json`` (the data catalog)
* Metric names  -> parses ``metrics/contracts/tenant_daily_metrics.json``
* SLO names     -> parses the SLO table in ``docs/slo-catalog.md``
"""

from __future__ import annotations

import importlib
import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = PROJECT_ROOT / "contracts" / "data_products"

REGISTRY_PATH = CONTRACTS_DIR / "registry.yml"
CONSUMERS_PATH = CONTRACTS_DIR / "consumers.yml"
REQUIREMENTS_PATH = CONTRACTS_DIR / "requirements.yml"

METRIC_CONTRACTS_PATH = PROJECT_ROOT / "metrics" / "contracts" / "tenant_daily_metrics.json"
EVENT_REGISTRY_PATH = PROJECT_ROOT / "contracts" / "registry.json"
DATA_CATALOG_PATH = PROJECT_ROOT / "catalog" / "data_catalog.json"
SLO_CATALOG_PATH = PROJECT_ROOT / "docs" / "slo-catalog.md"

REQUIREMENT_ID_PATTERN = re.compile(r"^[A-Z]+-\d{3}$")


# ---------------------------------------------------------------------------
# Contract loaders (data_products/ contracts)
# ---------------------------------------------------------------------------


def load_registry(path: Path = REGISTRY_PATH) -> dict[str, Any]:
    return yaml.safe_load(path.read_text())


def load_consumers(path: Path = CONSUMERS_PATH) -> dict[str, Any]:
    return yaml.safe_load(path.read_text())


def load_requirements(path: Path = REQUIREMENTS_PATH) -> dict[str, Any]:
    return yaml.safe_load(path.read_text())


def products_by_id(registry: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    registry = registry or load_registry()
    return {p["product_id"]: p for p in registry["data_products"]}


def consumers_by_id(consumers: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    consumers = consumers or load_consumers()
    return {c["consumer_id"]: c for c in consumers["consumers"]}


def requirements_by_id(requirements: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    requirements = requirements or load_requirements()
    return {r["requirement_id"]: r for r in requirements["requirements"]}


# ---------------------------------------------------------------------------
# Real-system fact loaders (what the contracts are cross-referenced against)
# ---------------------------------------------------------------------------


def load_metric_contract_names() -> set[str]:
    """Metric names defined in the technical formula contract
    (metrics/contracts/tenant_daily_metrics.json) — this registry never
    redefines a formula, only references it.
    """
    contract = json.loads(METRIC_CONTRACTS_PATH.read_text())
    return {c["metric"] for c in contract["contracts"]}


def load_source_event_types() -> set[str]:
    """Every event_type registered as a producible/consumable subject in
    contracts/registry.json.
    """
    registry = json.loads(EVENT_REGISTRY_PATH.read_text())
    event_types: set[str] = set()
    for subject in registry["subjects"]:
        event_types.update(subject["event_types"])
    return event_types


def load_catalog_table_names() -> set[str]:
    """Every table name registered in catalog/data_catalog.json."""
    catalog = json.loads(DATA_CATALOG_PATH.read_text())
    return {t["name"] if isinstance(t, dict) else t for t in catalog["tables"]}


def load_slo_names() -> set[str]:
    """Every SLO name in docs/slo-catalog.md's numbered SLO table — parsed
    from the markdown table itself, not a separately hand-maintained list.
    """
    content = SLO_CATALOG_PATH.read_text()
    names: set[str] = set()
    for line in content.splitlines():
        match = re.match(r"^\|\s*\d+\s*\|\s*([^|]+?)\s*\|", line)
        if match:
            names.add(match.group(1).strip())
    return names


def import_service_app(service_name: str) -> Any:
    """Import a service's FastAPI app module the same way
    tests/test_api_contracts.py does, so route cross-reference checks
    validate against the real, live route table rather than a mirror list.
    """
    service_path = PROJECT_ROOT / "services" / service_name
    for module_name in list(sys.modules):
        if module_name == "app" or module_name.startswith("app."):
            del sys.modules[module_name]
    sys.path.insert(0, str(service_path))
    try:
        return importlib.import_module("app.main")
    finally:
        try:
            sys.path.remove(str(service_path))
        except ValueError:
            pass


def load_analytics_api_routes() -> set[str]:
    """Real route paths served by analytics-service, read from its live
    OpenAPI schema (not a hand-maintained mirror list).
    """
    module = import_service_app("analytics-service")
    openapi = module.app.openapi()
    return set(openapi["paths"])
