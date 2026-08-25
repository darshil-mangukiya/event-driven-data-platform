"""Runtime Schema Registry for the platform's Kafka event contracts.

This is deliberately NOT a replacement for the existing, source-controlled
JSON Schema contracts under `contracts/` (`contracts/registry.json`,
`contracts/schemas/v1|v2/*.schema.json`) or the offline governance check
`scripts/check_contract_compatibility.py` — those remain the checked-in,
human-reviewed source of truth for what a contract *should* look like.
This service is the *runtime* layer on top: a real, queryable API a
producer or CI job can call before publishing to confirm a schema is
actually registered and compatible, matching the flow:

    source-controlled JSON Schema
            |
            v
    Schema Registry (this service) <- bootstrapped from contracts/registry.json on startup
            |
            v
    producer validation (scripts/validate_schema_registry.py, SDK)
            |
            v
    Kafka
            |
            v
    consumer compatibility enforcement

Compatibility mode defaults to BACKWARD (matching `contracts/registry.json`'s
`"compatibility": "BACKWARD"`), configurable per subject. The comparison
algorithm itself lives in `platform_shared.schema_compatibility` — the same
code the offline `check_contract_compatibility.py` script uses, so the two
layers can never silently disagree.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, status
from platform_shared.config import service_settings
from platform_shared.database import Postgres
from platform_shared.logging import configure_logging
from platform_shared.metrics import InMemoryServiceMetrics, MetricsMiddleware
from platform_shared.schema_compatibility import check_compatibility
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from starlette.responses import Response

from app.repository import SchemaRegistryRepository

SERVICE_NAME = "schema-registry-service"
settings = service_settings(SERVICE_NAME)
configure_logging(SERVICE_NAME, settings.log_level)
request_metrics = InMemoryServiceMetrics()
postgres = Postgres(settings.database_url)
repository = SchemaRegistryRepository(postgres)

CONTRACTS_ROOT = Path("/app/contracts") if Path("/app/contracts").exists() else Path(__file__).resolve().parents[3] / "contracts"
DEFAULT_COMPATIBILITY = "BACKWARD"

app = FastAPI(
    title="Event-Driven Data Platform Schema Registry",
    description=(
        "A custom runtime Schema Registry for this platform's Kafka event "
        "contracts: subject registration, versioning, and BACKWARD/FORWARD/FULL "
        "compatibility enforcement over the source-controlled JSON Schema "
        "contracts under contracts/. Its REST resource naming (subjects/"
        "versions/compatibility/config) is modeled on the Confluent Schema "
        "Registry API's conventions for familiarity, but request/response "
        "payload shapes are custom (nested JSON Schema objects, not "
        "string-encoded schemas) — it is not a wire-compatible drop-in for "
        "Confluent, Redpanda, or Apicurio clients."
    ),
    version="0.1.0",
)
app.add_middleware(MetricsMiddleware, metrics=request_metrics, service_name=SERVICE_NAME)


class SchemaRegistrationRequest(BaseModel):
    schema_json: dict[str, Any] = Field(..., description="A JSON Schema document")
    registered_by: str | None = None


class CompatibilityCheckRequest(BaseModel):
    schema_json: dict[str, Any]


class SchemaVersionResponse(BaseModel):
    subject: str
    version: int
    schema_id: str
    schema_json: dict[str, Any]
    registered_at: str


class CompatibilityResult(BaseModel):
    subject: str
    compatibility_mode: str
    is_compatible: bool
    errors: list[str]
    checked_against_version: int | None


def _version_response(row: dict[str, Any]) -> SchemaVersionResponse:
    return SchemaVersionResponse(
        subject=row["subject"],
        version=row["version"],
        schema_id=str(row["schema_id"]),
        schema_json=row["schema_json"],
        registered_at=row["registered_at"].isoformat(),
    )


@app.on_event("startup")
async def startup() -> None:
    await postgres.connect()


@app.on_event("shutdown")
async def shutdown() -> None:
    await postgres.close()


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"service": SERVICE_NAME, "status": "ok", "environment": settings.environment}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/subjects")
async def list_subjects() -> list[str]:
    return await repository.list_subjects()


@app.get("/config/{subject}")
async def get_subject_config(subject: str) -> dict[str, Any]:
    subject_row = await repository.get_subject(subject)
    if subject_row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"unknown subject: {subject}")
    return {"subject": subject, "compatibilityLevel": subject_row["compatibility_mode"]}


@app.get("/subjects/{subject}/versions")
async def list_versions(subject: str) -> list[int]:
    subject_row = await repository.get_subject(subject)
    if subject_row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"unknown subject: {subject}")
    return await repository.list_versions(subject)


@app.get("/subjects/{subject}/versions/latest", response_model=SchemaVersionResponse)
async def get_latest_version(subject: str) -> SchemaVersionResponse:
    row = await repository.latest_version(subject)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"no versions registered for subject: {subject}")
    return _version_response(row)


@app.get("/subjects/{subject}/versions/{version}", response_model=SchemaVersionResponse)
async def get_version(subject: str, version: int) -> SchemaVersionResponse:
    row = await repository.get_version(subject, version)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"subject {subject} has no version {version}")
    return _version_response(row)


async def _evaluate_compatibility(subject: str, candidate_schema: dict[str, Any]) -> CompatibilityResult:
    subject_row = await repository.get_subject(subject)
    mode = subject_row["compatibility_mode"] if subject_row else DEFAULT_COMPATIBILITY
    latest = await repository.latest_version(subject)
    if latest is None:
        # Nothing registered yet for this subject — the first schema is
        # trivially compatible (there is nothing to be incompatible with).
        return CompatibilityResult(
            subject=subject, compatibility_mode=mode, is_compatible=True, errors=[], checked_against_version=None
        )
    errors = check_compatibility(latest["schema_json"], candidate_schema, mode)
    return CompatibilityResult(
        subject=subject,
        compatibility_mode=mode,
        is_compatible=not errors,
        errors=errors,
        checked_against_version=latest["version"],
    )


@app.post("/compatibility/subjects/{subject}/versions/latest", response_model=CompatibilityResult)
async def check_compatibility_endpoint(subject: str, request: CompatibilityCheckRequest) -> CompatibilityResult:
    result = await _evaluate_compatibility(subject, request.schema_json)
    await repository.record_compatibility_check(
        subject=subject,
        compatibility_mode=result.compatibility_mode,
        is_compatible=result.is_compatible,
        errors=result.errors,
        dry_run=True,
    )
    return result


@app.post("/subjects/{subject}/versions", response_model=SchemaVersionResponse)
async def register_schema(subject: str, request: SchemaRegistrationRequest, response: Response) -> SchemaVersionResponse:
    await repository.ensure_subject(subject, DEFAULT_COMPATIBILITY)

    # Idempotent registration: re-submitting the exact schema already
    # registered as the latest version for this subject is a no-op that
    # returns the existing version, not a new one — found live during this
    # verification (re-running the bootstrap script against
    # an already-bootstrapped registry was creating a fresh, redundant
    # version every time instead of recognizing nothing had changed).
    existing_latest = await repository.latest_version(subject)
    if existing_latest is not None and existing_latest["schema_json"] == request.schema_json:
        response.status_code = status.HTTP_200_OK
        return _version_response(existing_latest)

    result = await _evaluate_compatibility(subject, request.schema_json)
    await repository.record_compatibility_check(
        subject=subject,
        compatibility_mode=result.compatibility_mode,
        is_compatible=result.is_compatible,
        errors=result.errors,
        dry_run=False,
    )
    if not result.is_compatible:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": f"schema is not {result.compatibility_mode} compatible with the current latest version",
                "errors": result.errors,
            },
        )
    next_version = (result.checked_against_version or 0) + 1
    row = await repository.register_version(subject, next_version, request.schema_json, request.registered_by)
    response.status_code = status.HTTP_201_CREATED
    return _version_response(row)


@app.get("/registry/bootstrap-status")
async def bootstrap_status() -> dict[str, Any]:
    """Reports whether this registry instance has been seeded from the
    checked-in contracts/registry.json — the real "source-controlled
    schema -> runtime registry" bootstrap step, run once at container
    startup via scripts/validate_schema_registry.py --bootstrap (not
    automatically on every app startup, so a fresh registry state is
    always inspectable before it's populated).
    """
    registry_manifest_path = CONTRACTS_ROOT / "registry.json"
    manifest_subjects: list[str] = []
    if registry_manifest_path.exists():
        manifest = json.loads(registry_manifest_path.read_text())
        manifest_subjects = [s["subject"] for s in manifest.get("subjects", [])]
    registered_subjects = await repository.list_subjects()
    return {
        "manifest_path": str(registry_manifest_path),
        "manifest_subjects": manifest_subjects,
        "registered_subjects": registered_subjects,
        "bootstrapped": bool(registered_subjects) and set(manifest_subjects) <= set(registered_subjects),
    }
