"""A small, static runbook catalog derived from this project's real
existing runbooks (docs/RUNBOOK.md, docs/reliability.md,
docs/disaster-recovery-runbook.md) — the copilot recommends one of these
ids, it never invents runbook content.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Runbook:
    id: str
    title: str
    doc_reference: str
    applies_to: tuple[str, ...]


RUNBOOK_CATALOG: dict[str, Runbook] = {
    "RB-KAFKA-001": Runbook(
        id="RB-KAFKA-001",
        title="Poison event / contract-breaking event handling",
        doc_reference="docs/reliability.md#poison-event",
        applies_to=("poison_event", "schema_incompatibility"),
    ),
    "RB-KAFKA-002": Runbook(
        id="RB-KAFKA-002",
        title="Consumer lag / backlog recovery",
        doc_reference="docs/reliability.md#consumer-lag",
        applies_to=("consumer_lag", "backlog"),
    ),
    "RB-KAFKA-003": Runbook(
        id="RB-KAFKA-003",
        title="Consumer crash / interruption recovery",
        doc_reference="docs/reliability.md#consumer-interruption",
        applies_to=("consumer_crash", "consumer_interruption"),
    ),
    "RB-DB-001": Runbook(
        id="RB-DB-001",
        title="PostgreSQL outage / restore",
        doc_reference="docs/disaster-recovery-runbook.md#postgresql-backup--restore",
        applies_to=("db_outage", "postgres_unavailable"),
    ),
    "RB-CACHE-001": Runbook(
        id="RB-CACHE-001",
        title="Redis outage — fail-open cache degradation",
        doc_reference="docs/reliability.md#redis-outage",
        applies_to=("redis_outage", "cache_unavailable"),
    ),
    "RB-RECON-001": Runbook(
        id="RB-RECON-001",
        title="Reconciliation mismatch investigation",
        doc_reference="docs/reconciliation.md",
        applies_to=("reconciliation_mismatch",),
    ),
    "RB-DLQ-001": Runbook(
        id="RB-DLQ-001",
        title="DLQ growth / replay",
        doc_reference="docs/RUNBOOK.md",
        applies_to=("dlq_growth", "duplicate_event"),
    ),
}


def find_runbook(incident_type: str) -> Runbook | None:
    for runbook in RUNBOOK_CATALOG.values():
        if incident_type in runbook.applies_to:
            return runbook
    return None
