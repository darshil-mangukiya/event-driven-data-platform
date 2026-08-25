"""Lightweight evidence/documentation consistency checker.

Deliberately small — this is not a general doc linter. It checks three
concrete things that have actually gone stale in this repository before:

1. Every relative markdown link inside `README.md`, `docs/*.md`, and
   `evidence/**/*.md` that points at another file in this repo resolves to
   a file that actually exists.
2. Every `evidence/validation/*.md` file the evidence index
   (`evidence/README.md`) links to actually exists on disk (the reverse
   direction: broken/missing evidence rather than broken doc links).
3. A hardcoded pytest test-count number appearing in `README.md` or
   `docs/testing-strategy.md` (e.g. "287 tests", "363 tests collected")
   matches what `pytest --collect-only -q` actually reports right now —
   this repository has hit exactly this kind of drift multiple times
   across repository changes (a manually maintained count can become stale).

Usage:
    python scripts/validate_evidence_consistency.py [--pretty]

Exits non-zero if any check fails.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

MARKDOWN_LINK_RE = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
TEST_COUNT_RE = re.compile(
    r"\b(\d{2,4})\s+tests?\s+collected\b"
    r"|\bcollects?\s+\*\*(\d{2,4})\s+tests\*\*"
    r"|\b(\d{2,4})\s+tests\s*\(pytest\)"
)


def _doc_files() -> list[Path]:
    files = [PROJECT_ROOT / "README.md", PROJECT_ROOT / "CONTRIBUTING.md"]
    files += sorted((PROJECT_ROOT / "docs").glob("*.md"))
    files += sorted((PROJECT_ROOT / "evidence").rglob("*.md"))
    existing = [f for f in files if f.exists()]
    public: list[Path] = []
    for path in existing:
        ignored = subprocess.run(
            ["git", "check-ignore", "--quiet", str(path)],
            cwd=PROJECT_ROOT,
            capture_output=True,
            check=False,
        )
        # Outside a Git checkout (including unit-test temp directories),
        # check-ignore returns nonzero and the file remains in scope.
        if ignored.returncode != 0:
            public.append(path)
    return public


def check_relative_links() -> list[str]:
    errors: list[str] = []
    for doc in _doc_files():
        text = doc.read_text(errors="replace")
        for match in MARKDOWN_LINK_RE.finditer(text):
            target = match.group(1).split("#", 1)[0].strip()
            if not target or target.startswith(("http://", "https://", "mailto:")):
                continue
            resolved = (doc.parent / target).resolve()
            if not resolved.exists():
                errors.append(f"{doc.relative_to(PROJECT_ROOT)}: broken link -> {target}")
    return errors


def check_evidence_index_targets_exist() -> list[str]:
    errors: list[str] = []
    index_path = PROJECT_ROOT / "evidence" / "README.md"
    if not index_path.exists():
        return [f"missing evidence index: {index_path.relative_to(PROJECT_ROOT)}"]
    text = index_path.read_text()
    for match in MARKDOWN_LINK_RE.finditer(text):
        target = match.group(1).split("#", 1)[0].strip()
        if not target or target.startswith(("http://", "https://")):
            continue
        resolved = (index_path.parent / target).resolve()
        if not resolved.exists():
            errors.append(f"evidence/README.md references missing file: {target}")
    return errors


def _actual_collected_test_count() -> int | None:
    try:
        completed = subprocess.run(
            # sys.executable, not the bare "python" — this must run pytest
            # using the SAME interpreter this script itself is running
            # under (e.g. .venv/bin/python), not whatever "python"
            # happens to resolve to first on $PATH (on this machine,
            # system Anaconda Python, with a different set of installed
            # packages and therefore a different, wrong test-collection
            # count). See
            # evidence/validation/application-rls-runtime-verification.md
            # and tests/test_evidence_consistency.py.
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
    except Exception:
        return None
    match = re.search(r"(\d+)\s+tests? collected", completed.stdout)
    return int(match.group(1)) if match else None


def check_test_counts_match_reality() -> list[str]:
    errors: list[str] = []
    actual = _actual_collected_test_count()
    if actual is None:
        return ["could not determine actual collected test count (pytest --collect-only failed to run)"]

    for doc in (PROJECT_ROOT / "README.md", PROJECT_ROOT / "docs" / "testing-strategy.md"):
        if not doc.exists():
            continue
        text = doc.read_text()
        for match in TEST_COUNT_RE.finditer(text):
            claimed = int(next(g for g in match.groups() if g is not None))
            # README's "N tests (pytest)" line describes the whole suite;
            # testing-strategy.md's collected count is the same number.
            # Anything more than a handful off is worth flagging — small
            # drift (a test added between doc-write and doc-check) is
            # normal; the point is catching a number frozen from 2+
            # earlier, without requiring exact-to-the-commit precision.
            if abs(claimed - actual) > 5:
                errors.append(
                    f"{doc.relative_to(PROJECT_ROOT)}: claims {claimed} tests, "
                    f"actual collected count is {actual}"
                )
    return errors


def run_all_checks() -> dict[str, object]:
    checks = {
        "relative_links": check_relative_links(),
        "evidence_index_targets": check_evidence_index_targets_exist(),
        "test_counts": check_test_counts_match_reality(),
    }
    all_errors = [e for errs in checks.values() for e in errs]
    return {
        "status": "passed" if not all_errors else "failed",
        "checks": {name: {"status": "passed" if not errs else "failed", "errors": errs} for name, errs in checks.items()},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Check documentation/evidence consistency.")
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--skip-test-count", action="store_true", help="Skip the pytest --collect-only cross-check (slow).")
    args = parser.parse_args()

    report = run_all_checks()
    if args.skip_test_count:
        report["checks"]["test_counts"] = {"status": "skipped", "errors": []}
        report["status"] = "passed" if all(
            c["status"] in ("passed", "skipped") for c in report["checks"].values()
        ) else "failed"

    print(json.dumps(report, indent=2 if args.pretty else None, sort_keys=True))
    if report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
