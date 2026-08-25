"""Tests for scripts/validate_evidence_consistency.py.

This is a lightweight documentation/evidence linter, not a general one —
see the script's own module docstring for exactly what it checks and why
those three checks specifically (this repo has gone stale in exactly these
ways before: broken doc links, a missing evidence file, and a hand-written
test count frozen in an older record).
"""

from __future__ import annotations

import importlib.util
import inspect
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "validate_evidence_consistency", PROJECT_ROOT / "scripts" / "validate_evidence_consistency.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


validator = _load()


def test_check_relative_links_flags_a_broken_link(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(validator, "PROJECT_ROOT", tmp_path)
    (tmp_path / "docs").mkdir()
    (tmp_path / "evidence").mkdir()
    (tmp_path / "README.md").write_text("See [nope](docs/does-not-exist.md) for detail.\n")
    errors = validator.check_relative_links()
    assert any("does-not-exist.md" in e for e in errors)


def test_check_relative_links_passes_for_a_real_link(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(validator, "PROJECT_ROOT", tmp_path)
    (tmp_path / "docs").mkdir()
    (tmp_path / "evidence").mkdir()
    (tmp_path / "docs" / "real.md").write_text("hello\n")
    (tmp_path / "README.md").write_text("See [real](docs/real.md) for detail.\n")
    errors = validator.check_relative_links()
    assert errors == []


def test_check_relative_links_ignores_external_urls(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(validator, "PROJECT_ROOT", tmp_path)
    (tmp_path / "docs").mkdir()
    (tmp_path / "evidence").mkdir()
    (tmp_path / "README.md").write_text("See [external](https://example.com/whatever) for detail.\n")
    errors = validator.check_relative_links()
    assert errors == []


def test_check_evidence_index_targets_exist_flags_missing_evidence_file(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(validator, "PROJECT_ROOT", tmp_path)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    (evidence_dir / "README.md").write_text("See [x](validation/missing.md).\n")
    errors = validator.check_evidence_index_targets_exist()
    assert any("missing.md" in e for e in errors)


def test_real_repository_passes_the_link_checks() -> None:
    """The actual repository, right now, should have zero broken relative
    links and zero missing evidence-index targets — this is the
    regression test that would have caught the exact drift this final
    hardening pass fixed (a README/evidence link to a file that didn't
    exist yet).
    """
    link_errors = validator.check_relative_links()
    evidence_errors = validator.check_evidence_index_targets_exist()
    assert link_errors == [], link_errors
    assert evidence_errors == [], evidence_errors


# --- Regression for the P6 "Final Application RLS + Validator Closeout" pass --
#
# Previously, _actual_collected_test_count() shelled out to the bare
# command "python", which on a machine where "python" resolves to a
# different interpreter than the one actually running this script (e.g.
# system Anaconda Python instead of .venv/bin/python) silently
# collected against the wrong environment and produced a wrong count
# (394 vs. the real 449 on this machine) — a false positive/negative
# entirely independent of whether the documented test count was
# stale.


def test_actual_collected_test_count_uses_sys_executable_not_bare_python() -> None:
    """No literal "python" executable invocation remains in the
    subprocess call — it must use sys.executable, so it always runs
    pytest with the same interpreter (and therefore the same installed
    packages) this validator script itself is running under.
    """
    source = inspect.getsource(validator._actual_collected_test_count)
    # Checks the actual subprocess argv list literal, not the whole source
    # text (which legitimately mentions "python" in an explanatory
    # comment) — the bug this guards against is `["python", "-m", ...]`.
    assert '["python",' not in source, (
        "found a bare ['python', ...] subprocess argv in "
        "_actual_collected_test_count — must use sys.executable instead"
    )
    assert "[sys.executable," in source


def test_actual_collected_test_count_subprocess_call_is_sys_executable_m_pytest(monkeypatch) -> None:
    """Directly asserts the argv passed to subprocess.run: [sys.executable,
    '-m', 'pytest', '--collect-only', '-q'] — not asserting behavior via
    the (slow, real) subprocess, but the actual command that would run.
    """
    captured: dict[str, object] = {}

    def fake_run(argv, **kwargs):
        captured["argv"] = argv
        result = MagicMock()
        result.stdout = "7 tests collected in 0.10s"
        return result

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(validator, "subprocess", subprocess)
    count = validator._actual_collected_test_count()

    assert captured["argv"][0] == sys.executable
    assert captured["argv"][1:4] == ["-m", "pytest", "--collect-only"]
    assert count == 7


def test_actual_collected_test_count_propagates_failure_as_none(monkeypatch) -> None:
    """Failure propagation must still work after the sys.executable
    change: a subprocess that raises still yields None, not an
    unhandled exception (check_test_counts_match_reality() depends on
    this to report a clean error instead of crashing).
    """

    def fake_run(argv, **kwargs):
        raise OSError("no such file or directory")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(validator, "subprocess", subprocess)
    assert validator._actual_collected_test_count() is None
    errors = validator.check_test_counts_match_reality()
    assert errors == ["could not determine actual collected test count (pytest --collect-only failed to run)"]


def test_validator_collected_count_matches_direct_venv_pytest_collection() -> None:
    """End-to-end check: run the validator's own
    _actual_collected_test_count() for real, and separately run
    `sys.executable -m pytest --collect-only -q` directly — both must
    report the identical number, because both now use the same
    interpreter.
    """
    from_validator = validator._actual_collected_test_count()
    assert from_validator is not None

    direct = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q"],
        cwd=PROJECT_ROOT,
        env={
            **__import__("os").environ,
            "PYTHONPATH": f"{PROJECT_ROOT}:{PROJECT_ROOT / 'services' / 'shared'}",
        },
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    import re as _re

    match = _re.search(r"(\d+)\s+tests? collected", direct.stdout)
    assert match is not None, direct.stdout
    assert from_validator == int(match.group(1))


def test_test_count_regex_matches_the_pytest_pytest_style_and_bold_style() -> None:
    text_a = "363 tests collected in 6.27s"
    text_b = "`pytest --collect-only -q` collects **363 tests**"
    text_c = "324 tests (pytest), `pytest-cov` wired in"
    for text in (text_a, text_b, text_c):
        match = validator.TEST_COUNT_RE.search(text)
        assert match is not None, text
        claimed = int(next(g for g in match.groups() if g is not None))
        assert claimed in (363, 324)
