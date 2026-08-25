"""Data lineage: graph analysis over catalog/data_catalog.json's
upstream/downstream declarations, plus real lineage-event emission wired
into actual pipeline runs (backfill, reconciliation, Structured Streaming).

Three things live here, deliberately kept separate from what already
existed previously:

* ``graph.py`` — loads the catalog as a directed graph, detects cycles and
  orphan tables, and cross-references *specific, checkable* lineage claims
  (e.g. "table X feeds analytics.metrics_api") against the actual service
  code that would have to read that table for the claim to be true. This
  is what caught three genuine pre-existing catalog/reality mismatches —
  see docs/lineage.md "What this framework itself caught".
* ``events.py`` — thin helpers reused by real pipeline code (backfill,
  reconciliation, the streaming job's sink) to emit an actual
  ``lineage_events`` row correlated by the same run_id as that pipeline's
  own ``pipeline_run_log`` / ``stream_processing_runs`` row, closing the
  gap ``docs/openlineage-tracking.md`` had flagged as a "production
  evolution" item that nothing actually implemented yet.

This does not replace or duplicate ``scripts/emit_lineage_event.py`` (the
manual CLI emitter) or ``catalog/data_catalog.json`` (the source of truth
for the graph) — it extends both.
"""
