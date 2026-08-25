# Incident Simulation

Deterministic incident scenarios live in `incidents/scenarios.json`.

Run a drill:

```bash
python scripts/incident_drill.py --pretty
```

Run one scenario:

```bash
python scripts/incident_drill.py --incident-id inc_metric_drift_detected --pretty
```

The drill output includes:

- severity
- owner
- signals
- estimated SLO burn rate
- response timeline
- postmortem requirement

Use `docs/postmortem-template.md` after a SEV1/SEV2 drill.
