"""Observability tests: metric registration, alert rule validity,
Prometheus/Grafana config validity, SLO definitions, reliability-to-
observability mapping, and no-duplicate-metric-registration.

These tests verify the *structure and consistency* of the observability
layer — they do not start Prometheus/Grafana or scrape live metrics (see
docs/OBSERVABILITY.md "Grafana Dashboard" for the live-rendering caveat).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MONITORING = PROJECT_ROOT / "monitoring"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def all_alert_names(groups: list[dict]) -> list[str]:
    names = []
    for group in groups:
        for rule in group.get("rules", []):
            if "alert" in rule:
                names.append(rule["alert"])
    return names


# ---------------------------------------------------------------------------
# Prometheus configuration
# ---------------------------------------------------------------------------


def test_prometheus_config_valid_yaml() -> None:
    """prometheus.yml must parse as valid YAML with expected top-level keys."""
    config = load_yaml(MONITORING / "prometheus.yml")
    assert "global" in config
    assert "scrape_configs" in config
    assert "rule_files" in config


def test_prometheus_scrape_targets_cover_all_services() -> None:
    """Every scraped service must have a job entry."""
    config = load_yaml(MONITORING / "prometheus.yml")
    job_names = {sc["job_name"] for sc in config["scrape_configs"]}
    expected = {
        "analytics-service",
        "ingestion-service",
        "processing-service",
        "metadata-service",
        "demo-dashboard",
        "ops-console",
        "spark-streaming",
    }
    assert expected.issubset(job_names), f"missing jobs: {expected - job_names}"


def test_prometheus_rule_files_exist() -> None:
    """Every rule file referenced in prometheus.yml must exist on disk."""
    config = load_yaml(MONITORING / "prometheus.yml")
    for rule_file in config.get("rule_files", []):
        # Rule files are referenced as /etc/prometheus/... in the container,
        # but on disk they're in monitoring/.
        filename = Path(rule_file).name
        assert (MONITORING / filename).exists(), f"rule file not found: {filename}"


# ---------------------------------------------------------------------------
# Alert rules
# ---------------------------------------------------------------------------


def test_alert_rules_valid_yaml() -> None:
    data = load_yaml(MONITORING / "alert_rules.yml")
    assert "groups" in data


def test_alert_rules_all_have_required_fields() -> None:
    data = load_yaml(MONITORING / "alert_rules.yml")
    for group in data["groups"]:
        assert "name" in group
        for rule in group.get("rules", []):
            assert "alert" in rule, f"rule in group {group['name']} missing 'alert'"
            assert "expr" in rule, f"alert {rule.get('alert')} missing 'expr'"
            assert "labels" in rule, f"alert {rule.get('alert')} missing 'labels'"
            assert "severity" in rule["labels"], f"alert {rule['alert']} missing severity label"
            assert rule["labels"]["severity"] in ("warning", "critical"), (
                f"alert {rule['alert']} has unexpected severity: {rule['labels']['severity']}"
            )


def test_alert_rules_no_duplicate_names() -> None:
    data = load_yaml(MONITORING / "alert_rules.yml")
    names = all_alert_names(data["groups"])
    assert len(names) == len(set(names)), f"duplicate alert names: {[n for n in names if names.count(n) > 1]}"


def test_alert_rules_have_threshold_rationale() -> None:
    """Every streaming/reliability/dependency alert should document why
    its threshold is what it is (annotations.threshold_rationale).
    """
    data = load_yaml(MONITORING / "alert_rules.yml")
    groups_needing_rationale = {"cloudscale-streaming", "cloudscale-dependencies", "cloudscale-reliability"}
    for group in data["groups"]:
        if group["name"] not in groups_needing_rationale:
            continue
        for rule in group.get("rules", []):
            annotations = rule.get("annotations", {})
            assert "threshold_rationale" in annotations, (
                f"alert {rule['alert']} in group {group['name']} missing threshold_rationale"
            )


def test_alert_rules_streaming_group_exists() -> None:
    data = load_yaml(MONITORING / "alert_rules.yml")
    group_names = {g["name"] for g in data["groups"]}
    assert "cloudscale-streaming" in group_names


def test_alert_rules_dependencies_group_exists() -> None:
    data = load_yaml(MONITORING / "alert_rules.yml")
    group_names = {g["name"] for g in data["groups"]}
    assert "cloudscale-dependencies" in group_names


def test_alert_rules_reliability_group_exists() -> None:
    data = load_yaml(MONITORING / "alert_rules.yml")
    group_names = {g["name"] for g in data["groups"]}
    assert "cloudscale-reliability" in group_names


# ---------------------------------------------------------------------------
# SLO rules
# ---------------------------------------------------------------------------


def test_slo_rules_valid_yaml() -> None:
    data = load_yaml(MONITORING / "slo_rules.yml")
    assert "groups" in data


def test_slo_rules_no_overlap_with_alert_rules() -> None:
    """SLO rules and alert rules shouldn't define the same alert name."""
    alert_data = load_yaml(MONITORING / "alert_rules.yml")
    slo_data = load_yaml(MONITORING / "slo_rules.yml")
    alert_names = set(all_alert_names(alert_data["groups"]))
    slo_names = set(all_alert_names(slo_data["groups"]))
    overlap = alert_names & slo_names
    assert not overlap, f"duplicate alert names across rule files: {overlap}"


# ---------------------------------------------------------------------------
# Grafana dashboard
# ---------------------------------------------------------------------------


def test_grafana_dashboard_valid_json() -> None:
    dashboard = load_json(MONITORING / "grafana_dashboard.json")
    assert "panels" in dashboard
    assert "title" in dashboard


def test_grafana_dashboard_all_panels_have_required_fields() -> None:
    dashboard = load_json(MONITORING / "grafana_dashboard.json")
    for panel in dashboard["panels"]:
        assert "id" in panel, "panel missing 'id'"
        assert "type" in panel, f"panel {panel.get('id')} missing 'type'"
        assert "gridPos" in panel, f"panel {panel.get('id')} missing 'gridPos'"
        if panel["type"] != "row":
            assert "targets" in panel, f"panel {panel.get('id')} ({panel.get('title')}) missing 'targets'"


def test_grafana_dashboard_no_duplicate_panel_ids() -> None:
    dashboard = load_json(MONITORING / "grafana_dashboard.json")
    ids = [p["id"] for p in dashboard["panels"]]
    assert len(ids) == len(set(ids)), f"duplicate panel ids: {[i for i in ids if ids.count(i) > 1]}"


def test_grafana_dashboard_has_streaming_row() -> None:
    dashboard = load_json(MONITORING / "grafana_dashboard.json")
    row_titles = [p.get("title", "") for p in dashboard["panels"] if p.get("type") == "row"]
    assert any("streaming" in t.lower() for t in row_titles), f"no streaming row found; rows: {row_titles}"


def test_grafana_dashboard_has_reliability_row() -> None:
    dashboard = load_json(MONITORING / "grafana_dashboard.json")
    row_titles = [p.get("title", "") for p in dashboard["panels"] if p.get("type") == "row"]
    assert any("reliab" in t.lower() or "reconcil" in t.lower() for t in row_titles), (
        f"no reliability/reconciliation row found; rows: {row_titles}"
    )


def test_grafana_dashboard_panels_reference_real_metrics() -> None:
    """Every PromQL expression in the dashboard should reference a metric
    that exists in the platform's metric inventory.
    """
    known_metric_prefixes = {
        "platform_api_requests_total",
        "platform_api_request_latency_seconds",
        "platform_kafka_events_published_total",
        "platform_kafka_events_processed_total",
        "platform_cache_events_total",
        "platform_cache_available",
        "cloudscale_stream_events_received_total",
        "cloudscale_stream_events_processed_total",
        "cloudscale_stream_events_failed_total",
        "cloudscale_stream_events_duplicate_total",
        "cloudscale_stream_events_late_total",
        "cloudscale_stream_dlq_total",
        "cloudscale_stream_batches_total",
        "cloudscale_stream_batch_duration_seconds",
        "cloudscale_stream_processing_lag_seconds",
        "cloudscale_stream_watermark_lag_seconds",
        "cloudscale_stream_sink_failures_total",
        "cloudscale_stream_checkpoint_age_seconds",
        "cloudscale_stream_records_per_batch",
        "cloudscale_stream_postgres_available",
        "cloudscale_reliability_exercise_last_status",
        "cloudscale_reliability_exercise_last_run_age_seconds",
        "cloudscale_reconciliation_recent_failures",
        "cloudscale_reconciliation_recent_checks",
        "cloudscale_stream_checkpoint_freshness_seconds_db",
        "cloudscale_stream_late_events_recent",
        "cloudscale_serving_metrics_staleness_seconds",
        # PromQL built-ins
        "time",
        "up",
    }
    dashboard = load_json(MONITORING / "grafana_dashboard.json")
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            expr = target.get("expr", "")
            # Extract metric names from the PromQL expression
            metric_refs = re.findall(r"[a-z][a-z0-9_]+(?:_bucket|_count|_sum|_total)?", expr)
            for ref in metric_refs:
                # Strip histogram suffixes for matching
                base = re.sub(r"_(bucket|count|sum)$", "", ref)
                # Skip PromQL function names and label values
                if base in ("sum", "rate", "increase", "histogram_quantile", "by", "le", "hit", "miss",
                            "unavailable", "success", "failed", "redis", "on_time", "service", "cache",
                            "outcome", "query", "topic", "event_domain", "reason", "classification",
                            "sink", "scenario_id", "table", "status_code", "method", "path",
                            "status", "event_type", "for", "warning", "critical"):
                    continue
                assert any(base.startswith(known) or known.startswith(base) for known in known_metric_prefixes), (
                    f"panel {panel.get('id')} ({panel.get('title')}) references unknown metric: {ref}"
                )


def test_grafana_dashboard_minimum_panel_count() -> None:
    """Dashboard should have a meaningful number of panels (beyond placeholders)."""
    dashboard = load_json(MONITORING / "grafana_dashboard.json")
    non_row_panels = [p for p in dashboard["panels"] if p["type"] != "row"]
    assert len(non_row_panels) >= 20, f"expected >= 20 data panels, got {len(non_row_panels)}"


# ---------------------------------------------------------------------------
# Grafana provisioning
# ---------------------------------------------------------------------------


def test_grafana_provisioning_datasource_exists() -> None:
    ds_path = MONITORING / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
    assert ds_path.exists()
    ds = yaml.safe_load(ds_path.read_text())
    assert ds["datasources"][0]["type"] == "prometheus"


def test_grafana_provisioning_dashboard_exists() -> None:
    db_path = MONITORING / "grafana" / "provisioning" / "dashboards" / "default.yml"
    assert db_path.exists()
    db = yaml.safe_load(db_path.read_text())
    assert db["providers"][0]["options"]["path"] == "/var/lib/grafana/dashboards"


# ---------------------------------------------------------------------------
# Metric registration (no duplicates across modules)
# ---------------------------------------------------------------------------


def test_no_duplicate_metric_names_across_modules() -> None:
    """Metrics defined in platform_shared/metrics.py, spark/streaming/metrics.py,
    and ops-console/observability.py must not collide.

    ops-console/observability.py defines its Gauges via a local ``_gauge()``
    get-or-create wrapper , not a bare ``Gauge(...)`` call — see
    that module's docstring for why (test-harness re-import safety). The
    wrapper's own call signature — name, documentation, labelnames — is
    identical to Gauge's, so it's recognized here the same way.
    """
    import ast

    metric_names: dict[str, str] = {}  # metric_name -> source_file
    files = [
        PROJECT_ROOT / "services" / "shared" / "platform_shared" / "metrics.py",
        PROJECT_ROOT / "spark" / "streaming" / "metrics.py",
        PROJECT_ROOT / "services" / "ops-console" / "app" / "observability.py",
    ]
    metric_constructor_names = ("Counter", "Gauge", "Histogram", "Summary", "_gauge")
    for filepath in files:
        tree = ast.parse(filepath.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id in metric_constructor_names:
                    if node.args and isinstance(node.args[0], ast.Constant):
                        name = node.args[0].value
                        source = filepath.relative_to(PROJECT_ROOT)
                        assert name not in metric_names, (
                            f"duplicate metric '{name}': defined in both "
                            f"{metric_names[name]} and {source}"
                        )
                        metric_names[name] = str(source)
    # Sanity: we should have found a meaningful number of metrics
    assert len(metric_names) >= 25, f"only found {len(metric_names)} metrics — AST parse may be broken"


def test_streaming_metrics_use_cloudscale_namespace() -> None:
    """All streaming metrics must use the cloudscale_stream_ prefix, not
    platform_ — the two namespaces are deliberately distinct (see
    spark/streaming/metrics.py docstring).
    """
    import ast

    tree = ast.parse((PROJECT_ROOT / "spark" / "streaming" / "metrics.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in ("Counter", "Gauge", "Histogram") and node.args:
                if isinstance(node.args[0], ast.Constant):
                    name = node.args[0].value
                    assert name.startswith("cloudscale_stream_"), (
                        f"streaming metric '{name}' should start with 'cloudscale_stream_'"
                    )


def test_label_cardinality_no_high_cardinality_labels() -> None:
    """Ensure no metric uses known high-cardinality labels."""
    import ast

    dangerous_labels = {"event_id", "trace_id", "order_id", "customer_id",
                        "correlation_id", "causation_id", "idempotency_key",
                        "error_message", "error_text", "raw_value", "payload"}
    files = [
        PROJECT_ROOT / "services" / "shared" / "platform_shared" / "metrics.py",
        PROJECT_ROOT / "spark" / "streaming" / "metrics.py",
        PROJECT_ROOT / "services" / "ops-console" / "app" / "observability.py",
    ]
    for filepath in files:
        tree = ast.parse(filepath.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id in ("Counter", "Gauge", "Histogram"):
                    # Check label names (third positional arg is a list of label names)
                    if len(node.args) >= 3 and isinstance(node.args[2], ast.List):
                        for elt in node.args[2].elts:
                            if isinstance(elt, ast.Constant):
                                assert elt.value not in dangerous_labels, (
                                    f"metric in {filepath.name} uses high-cardinality label '{elt.value}'"
                                )


# ---------------------------------------------------------------------------
# Reliability → Observability mapping
# ---------------------------------------------------------------------------


def test_reliability_scenarios_have_detection_method() -> None:
    """Every reliability scenario's ScenarioResult must include a non-empty
    detection_method field that references a real metric or alert.
    """
    from reliability.scenarios import REGISTRY

    for scenario_id, module in REGISTRY.items():
        from spark.streaming.config import StreamingConfig
        config = StreamingConfig()
        result = module.run(config)
        assert result.detection_method, f"scenario {scenario_id} has empty detection_method"
        # Must reference at least one metric name or alert name
        assert any(
            ref in result.detection_method
            for ref in ("cloudscale_", "platform_", "Streaming", "Redis", "Reconciliation",
                        "processing", "checkpoint", "reconciliation_audit", "evaluate_reconciliation")
        ), f"scenario {scenario_id} detection_method doesn't reference a known metric/alert: {result.detection_method}"


# ---------------------------------------------------------------------------
# Docker Compose
# ---------------------------------------------------------------------------


def test_docker_compose_has_grafana_service() -> None:
    compose = load_yaml(PROJECT_ROOT / "docker-compose.yml")
    assert "grafana" in compose["services"], "grafana service not found in docker-compose.yml"


def test_docker_compose_grafana_depends_on_prometheus() -> None:
    compose = load_yaml(PROJECT_ROOT / "docker-compose.yml")
    grafana = compose["services"]["grafana"]
    depends_on = grafana.get("depends_on", [])
    if isinstance(depends_on, list):
        assert "prometheus" in depends_on
    elif isinstance(depends_on, dict):
        assert "prometheus" in depends_on
    else:
        raise AssertionError("grafana depends_on has unexpected type")


def test_docker_compose_grafana_volume_exists() -> None:
    compose = load_yaml(PROJECT_ROOT / "docker-compose.yml")
    assert "grafana-data" in compose.get("volumes", {})


# ---------------------------------------------------------------------------
# SLO catalog
# ---------------------------------------------------------------------------


def test_slo_catalog_exists() -> None:
    assert (PROJECT_ROOT / "docs" / "slo-catalog.md").exists()


def test_slo_catalog_has_minimum_slos() -> None:
    """The SLO catalog should define at least 8 SLOs."""
    content = (PROJECT_ROOT / "docs" / "slo-catalog.md").read_text()
    # Count rows in the SLO table (lines starting with | followed by a number)
    slo_rows = re.findall(r"^\| \d+", content, re.MULTILINE)
    assert len(slo_rows) >= 8, f"expected >= 8 SLOs, found {len(slo_rows)}"


def test_slo_catalog_references_real_metrics() -> None:
    """SLO catalog should reference metrics that actually exist."""
    content = (PROJECT_ROOT / "docs" / "slo-catalog.md").read_text()
    metrics_referenced = re.findall(r"(cloudscale_\w+|platform_\w+)", content)
    known_prefixes = {"cloudscale_stream_", "cloudscale_serving_", "cloudscale_reconciliation_",
                      "cloudscale_reliability_", "platform_api_", "platform_cache_",
                      "platform_kafka_"}
    # Filter out non-metric references (module/package names like platform_shared)
    non_metrics = {"platform_shared", "platform_producer", "platform_cli"}
    for metric in metrics_referenced:
        if metric in non_metrics:
            continue
        assert any(metric.startswith(p) for p in known_prefixes), (
            f"SLO catalog references unknown metric prefix: {metric}"
        )
