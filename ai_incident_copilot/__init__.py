"""AI Streaming Incident Copilot.

The deterministic platform (reliability exercises, Prometheus alerts,
reconciliation checks) remains the sole source of truth for whether an
incident occurred. This copilot only runs *after* detection, to help a
human summarize, classify, and triage — it never detects anything itself,
and it never takes an action; see `AI_CONTROL_BOUNDARIES.md` in this
directory for its control boundaries.
"""

from __future__ import annotations
