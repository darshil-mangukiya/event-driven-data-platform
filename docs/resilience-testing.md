# Chaos and Resilience Testing

The local resilience probe does not destructively stop services. It checks expected dependencies and records what behavior should be verified during a controlled failure test.

Dry-run:

```bash
python scripts/resilience_probe.py --dry-run
```

Probe local targets:

```bash
python scripts/resilience_probe.py --output evidence/validation/resilience-probe.json
```

Scenario catalog:

- `analytics_service_unavailable`
- `redis_unavailable`
- `kafka_unavailable`
- `postgres_unavailable`

Production failure testing should be run in a staging environment with explicit rollback steps, alerts enabled, and a pre-approved maintenance window.
