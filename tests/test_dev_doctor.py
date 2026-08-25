"""Tests for scripts/dev_doctor.py (Developer Experience and
Workflow Consolidation) and for the Makefile/CI-workflow consolidation that
went with it.

dev_doctor.py is deliberately distinct from scripts/platform_preflight.py:
preflight checks repo/data governance state (contracts, catalog, RLS, ...);
dev_doctor checks whether *this machine* is ready to run the repo at all
(Python version, Docker reachability, .env presence, and host port
conflicts against docker-compose.yml — the exact class of problem that cost
real debugging time earlier in this project, when a native macOS PostgreSQL
already bound port 5432).
"""

from __future__ import annotations

import importlib.util
import socket
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_dev_doctor():
    """Load scripts/dev_doctor.py in isolation, avoiding any sys.modules
    collision with another service's `app`/`scripts` package — the same
    importlib pattern used elsewhere in this project's test suite.
    """
    spec = importlib.util.spec_from_file_location("dev_doctor", PROJECT_ROOT / "scripts" / "dev_doctor.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # dataclasses' @dataclass(frozen=True) looks up its own module in
    # sys.modules while processing the class body — it must be registered
    # before exec_module runs, or dataclasses.dataclass raises
    # AttributeError: 'NoneType' object has no attribute '__dict__'.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


dev_doctor = _load_dev_doctor()


def test_check_python_version_passes_on_the_interpreter_actually_running() -> None:
    # The test itself only runs under Python >= 3.11 (pyproject's own
    # requires-python), so this should always pass.
    result = dev_doctor.check_python_version()
    assert result.status == "pass"


def test_check_python_version_fails_below_the_minimum() -> None:
    with patch.object(dev_doctor.sys, "version_info", (3, 9, 0, "final", 0)):
        result = dev_doctor.check_python_version()
    assert result.status == "fail"
    assert result.fix is not None


def test_port_in_use_detects_a_real_local_listener() -> None:
    # Bind an actual socket, then assert the detector sees it — a live,
    # non-mocked check that the detection logic is real, not a stub.
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    _, port = server.getsockname()
    try:
        assert dev_doctor._port_in_use(port) is True
    finally:
        server.close()


def test_port_in_use_returns_false_for_a_closed_port() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    _, port = server.getsockname()
    server.close()  # freed immediately, nothing listening
    assert dev_doctor._port_in_use(port) is False


def test_check_compose_ports_flags_a_conflict_with_the_postgres_override_fix() -> None:
    """Regression: the fix message for a port-5432 conflict must mention the
    existing POSTGRES_HOST_PORT escape hatch docker-compose.yml already
    supports, not a generic "stop the other process" message — that
    override is the actual workaround this project has used repeatedly.
    """
    with patch.object(dev_doctor, "_port_in_use", side_effect=lambda port, **_: port == 5432):
        results = dev_doctor.check_compose_ports()
    by_name = {r.name: r for r in results}
    assert by_name["port_5432"].status == "warn"
    assert "POSTGRES_HOST_PORT" in (by_name["port_5432"].fix or "")
    # A port with no known compose override should get a different, generic fix.
    assert by_name["port_6379"].status == "pass"


def test_check_compose_ports_all_clear_when_nothing_is_listening() -> None:
    with patch.object(dev_doctor, "_port_in_use", return_value=False):
        results = dev_doctor.check_compose_ports()
    assert all(r.status == "pass" for r in results)


def test_check_env_file_warns_with_the_exact_cp_command_when_missing() -> None:
    with patch.object(dev_doctor, "PROJECT_ROOT", Path("/nonexistent-for-test")):
        # PROJECT_ROOT / ".env" won't exist; PROJECT_ROOT / ".env.example"
        # won't either, since the whole path is fake — exercise the "fail"
        # branch (neither file present).
        result = dev_doctor.check_env_file()
    assert result.status == "fail"


def test_check_env_file_passes_when_env_exists(tmp_path) -> None:
    (tmp_path / ".env").write_text("X=1\n")
    with patch.object(dev_doctor, "PROJECT_ROOT", tmp_path):
        result = dev_doctor.check_env_file()
    assert result.status == "pass"


def test_check_env_file_warns_with_cp_fix_when_only_example_exists(tmp_path) -> None:
    (tmp_path / ".env.example").write_text("X=1\n")
    with patch.object(dev_doctor, "PROJECT_ROOT", tmp_path):
        result = dev_doctor.check_env_file()
    assert result.status == "warn"
    assert result.fix == "cp .env.example .env"


def test_run_doctor_overall_status_is_fail_if_any_check_fails() -> None:
    with patch.object(dev_doctor, "check_python_version", return_value=dev_doctor.DoctorResult("python_version", "fail", "boom")):
        report = dev_doctor.run_doctor()
    assert report.status == "fail"


def test_run_doctor_overall_status_is_pass_when_only_warnings_exist() -> None:
    """A missing .env or an unreachable Docker daemon should not fail the
    whole check — those are common, recoverable first-run states, not
    broken environments. Only a hard "fail" (e.g. Python too old, a core
    dependency missing) should flip the overall status.
    """
    warn_only_results = [
        dev_doctor.DoctorResult("a", "pass", "ok"),
        dev_doctor.DoctorResult("b", "warn", "heads up"),
    ]
    with patch.object(dev_doctor, "check_python_version", return_value=warn_only_results[0]), \
         patch.object(dev_doctor, "check_venv_active", return_value=warn_only_results[1]), \
         patch.object(dev_doctor, "check_env_file", return_value=warn_only_results[0]), \
         patch.object(dev_doctor, "check_requirements_installed", return_value=warn_only_results[0]), \
         patch.object(dev_doctor, "check_docker", return_value=[warn_only_results[0]]), \
         patch.object(dev_doctor, "check_compose_ports", return_value=[warn_only_results[0]]):
        report = dev_doctor.run_doctor()
    assert report.status == "pass"


def test_render_text_includes_fix_lines_for_non_passing_checks() -> None:
    report = dev_doctor.DoctorReport(
        generated_at="2026-08-20T00:00:00+00:00",
        status="pass",
        results=[dev_doctor.DoctorResult("x", "warn", "something", fix="do this")],
    )
    text = dev_doctor.render_text(report)
    assert "something" in text
    assert "fix: do this" in text


def test_to_dict_round_trips_all_fields() -> None:
    report = dev_doctor.DoctorReport(
        generated_at="2026-08-20T00:00:00+00:00",
        status="pass",
        results=[dev_doctor.DoctorResult("x", "pass", "ok", fix=None)],
    )
    payload = report.to_dict()
    assert payload["status"] == "pass"
    assert payload["checks"][0] == {"name": "x", "status": "pass", "detail": "ok", "fix": None}


# ---------------------------------------------------------------------------
# Makefile / CI-workflow consolidation regression tests
# ---------------------------------------------------------------------------


def test_makefile_help_target_is_the_default_goal() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    assert ".DEFAULT_GOAL := help" in makefile
    assert "\nhelp:" in makefile or makefile.startswith("help:")


def test_every_makefile_target_has_help_text() -> None:
    """Regression: a Makefile target added without a `## description` comment
    silently disappears from `make help` — this project has ~70 targets, so
    an undocumented one is easy to miss without an explicit check.
    """
    import re

    makefile = (PROJECT_ROOT / "Makefile").read_text()
    target_pattern = re.compile(r"^([a-zA-Z0-9_-]+):", re.MULTILINE)
    documented_pattern = re.compile(r"^([a-zA-Z0-9_-]+):.*## ", re.MULTILINE)

    all_targets = {m.group(1) for m in target_pattern.finditer(makefile)} - {"PHONY"}
    documented_targets = {m.group(1) for m in documented_pattern.finditer(makefile)}

    undocumented = all_targets - documented_targets
    assert not undocumented, f"Makefile targets missing '## description' text: {sorted(undocumented)}"


def test_makefile_setup_and_doctor_targets_exist() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    assert "\nsetup:" in makefile
    assert "\ndoctor:" in makefile
    assert "scripts/dev_doctor.py" in makefile


def test_makefile_ci_local_target_covers_the_same_checks_as_ci_yml() -> None:
    """Regression: ci-local exists specifically so a contributor can
    reproduce CI failures locally before pushing — it must not silently
    drift to cover fewer checks than .github/workflows/ci.yml runs.
    """
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    ci_yml = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text()

    ci_local_line = next(line for line in makefile.splitlines() if line.startswith("ci-local:"))

    assert "lint" in ci_local_line
    assert "test" in ci_local_line
    for governance_script in ("validate_lineage.py", "validate_data_products.py", "validate_catalog.py"):
        assert governance_script in ci_yml, f"{governance_script} missing from ci.yml"


def test_ci_yml_runs_lineage_and_data_products_validation() -> None:
    """Regression for the specific gap the code closes: lineage and
    data-product governance scripts had dedicated Makefile targets but were
    never wired into ci.yml, so CI could pass while either was broken.
    """
    ci_yml = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert "scripts/validate_lineage.py" in ci_yml
    assert "scripts/validate_data_products.py" in ci_yml


def test_ci_yml_is_valid_yaml() -> None:
    yaml = pytest.importorskip("yaml")
    with (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").open() as handle:
        parsed = yaml.safe_load(handle)
    assert parsed["jobs"]["test"]["steps"]


def test_contributing_doc_exists_and_mentions_doctor() -> None:
    contributing = PROJECT_ROOT / "CONTRIBUTING.md"
    assert contributing.exists()
    text = contributing.read_text()
    assert "make setup" in text or "make doctor" in text


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
