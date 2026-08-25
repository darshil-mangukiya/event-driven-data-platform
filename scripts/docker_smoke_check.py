from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class EndpointCheck:
    name: str
    url: str
    expected_statuses: tuple[int, ...] = (200,)


@dataclass(frozen=True)
class CheckResult:
    name: str
    url: str
    ok: bool
    status_code: int | None = None
    error: str | None = None


def base_url(env_name: str, default: str) -> str:
    return os.getenv(env_name, default).rstrip("/")


def build_checks(args: argparse.Namespace) -> list[EndpointCheck]:
    ingestion_url = base_url("INGESTION_SERVICE_URL", args.ingestion_url)
    analytics_url = base_url("ANALYTICS_SERVICE_URL", args.analytics_url)
    metadata_url = base_url("METADATA_SERVICE_URL", args.metadata_url)
    checks = [
        EndpointCheck("ingestion health", f"{ingestion_url}/health"),
        EndpointCheck("ingestion openapi", f"{ingestion_url}/docs"),
        EndpointCheck("analytics health", f"{analytics_url}/health"),
        EndpointCheck("analytics openapi", f"{analytics_url}/docs"),
        EndpointCheck("metadata health", f"{metadata_url}/health"),
        EndpointCheck("metadata openapi", f"{metadata_url}/docs"),
    ]
    if args.include_airflow:
        airflow_url = base_url("AIRFLOW_URL", args.airflow_url)
        checks.append(EndpointCheck("airflow health", f"{airflow_url}/health"))
    if args.include_prometheus:
        prometheus_url = base_url("PROMETHEUS_URL", args.prometheus_url)
        checks.append(EndpointCheck("prometheus health", f"{prometheus_url}/-/healthy"))
    return checks


def run_check(check: EndpointCheck, timeout: float) -> CheckResult:
    request = Request(check.url, headers={"User-Agent": "cloudscale-docker-smoke-check"})
    try:
        with urlopen(request, timeout=timeout) as response:
            status_code = response.getcode()
    except HTTPError as exc:
        return CheckResult(
            name=check.name,
            url=check.url,
            ok=exc.code in check.expected_statuses,
            status_code=exc.code,
            error=str(exc),
        )
    except (TimeoutError, URLError, OSError) as exc:
        return CheckResult(name=check.name, url=check.url, ok=False, error=str(exc))
    return CheckResult(
        name=check.name,
        url=check.url,
        ok=status_code in check.expected_statuses,
        status_code=status_code,
    )


def format_result(result: CheckResult) -> str:
    status = "PASS" if result.ok else "FAIL"
    detail = f"status={result.status_code}" if result.status_code is not None else f"error={result.error}"
    return f"[{status}] {result.name}: {result.url} ({detail})"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check local data-platform Docker services after Compose startup.")
    parser.add_argument("--ingestion-url", default="http://localhost:8001")
    parser.add_argument("--analytics-url", default="http://localhost:8003")
    parser.add_argument("--metadata-url", default="http://localhost:8004")
    parser.add_argument("--airflow-url", default="http://localhost:8088")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--include-airflow", action="store_true")
    parser.add_argument("--include-prometheus", action="store_true")
    parser.add_argument("--timeout", type=float, default=float(os.getenv("SMOKE_TIMEOUT_SECONDS", "3")))
    parser.add_argument("--json", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    results = [run_check(check, args.timeout) for check in build_checks(args)]
    if args.json:
        print(json.dumps([asdict(result) for result in results], indent=2, sort_keys=True))
    else:
        for result in results:
            print(format_result(result))

    failed = [result for result in results if not result.ok]
    if failed:
        print(f"Smoke check failed: {len(failed)} of {len(results)} checks failed.", file=sys.stderr)
        raise SystemExit(1)
    print(f"Smoke check passed: {len(results)} checks passed.")


if __name__ == "__main__":
    main()
