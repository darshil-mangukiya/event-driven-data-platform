"""Registry of runnable reliability scenarios.

Each module exposes ``SCENARIO_ID`` and a ``run(config=None) -> ScenarioResult``
function. ``REGISTRY`` maps id -> module so the CLI
(``platform_cli reliability run <id>``) and Makefile targets can dispatch
without a long if/elif chain.
"""

from __future__ import annotations

from reliability.scenarios import (
    consumer_interruption,
    consumer_lag,
    db_outage,
    duplicate_event,
    late_event,
    poison_event,
    reconciliation_mismatch,
    redis_outage,
)

REGISTRY = {
    poison_event.SCENARIO_ID: poison_event,
    duplicate_event.SCENARIO_ID: duplicate_event,
    late_event.SCENARIO_ID: late_event,
    consumer_lag.SCENARIO_ID: consumer_lag,
    db_outage.SCENARIO_ID: db_outage,
    redis_outage.SCENARIO_ID: redis_outage,
    reconciliation_mismatch.SCENARIO_ID: reconciliation_mismatch,
    consumer_interruption.SCENARIO_ID: consumer_interruption,
}
