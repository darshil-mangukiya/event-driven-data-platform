from __future__ import annotations

import json
import sys
from pathlib import Path

from platform_shared.schemas import envelope_from_json


def validate_samples(samples_root: Path) -> list[str]:
    errors: list[str] = []
    event_path = samples_root / "events" / "sample_events_v2.jsonl"
    if not event_path.exists():
        errors.append(f"missing {event_path}")
    else:
        for line_number, raw in enumerate(event_path.read_text().splitlines(), start=1):
            if not raw.strip():
                continue
            try:
                envelope_from_json(raw)
            except Exception as exc:
                errors.append(f"{event_path}:{line_number} invalid event envelope: {exc}")

    json_paths = [
        samples_root / "benchmarks" / "local_ingestion_sample.json",
        samples_root / "quality" / "tenant_demo_quality_sample.json",
        samples_root / "dashboard" / "tenant_demo_dashboard_sample.json",
    ]
    for path in json_paths:
        if not path.exists():
            errors.append(f"missing {path}")
            continue
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            errors.append(f"{path} invalid json: {exc}")
            continue
        if payload.get("sample_artifact") is not True:
            errors.append(f"{path} must set sample_artifact=true")

    return errors


def main() -> None:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("samples")
    errors = validate_samples(root)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        raise SystemExit(1)
    print(f"validated sample artifacts under {root}")


if __name__ == "__main__":
    main()
