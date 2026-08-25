# Deployment Hardening

## Secrets

- Store database, Redis, Kafka, and JWT secrets in a secrets manager.
- Do not bake secrets into Docker images or ConfigMaps.
- Rotate service credentials on a scheduled cadence.

## Config Validation

Every service should validate required environment variables at startup:

- database URL
- Kafka bootstrap servers
- Redis URL where required
- JWT secret or service identity config
- environment name

## Probes

Readiness should check dependencies needed to serve traffic. Liveness should only detect wedged processes.

| Service | Readiness | Liveness |
| --- | --- | --- |
| Ingestion | Kafka producer reachable | process responds |
| Processing | Kafka consumer and Postgres reachable | worker loop alive |
| Analytics | Postgres reachable; Redis optional degraded state | process responds |
| Metadata | Postgres reachable | process responds |

## Blue/Green Deployment

1. Deploy green stack with migrations already applied.
2. Run health, smoke, contract, and schema drift checks.
3. Mirror low-risk traffic if available.
4. Shift traffic gradually.
5. Keep blue stack warm until SLOs stabilize.

## Rollback

- Roll back application images first.
- Avoid down migrations unless explicitly tested.
- If schema changes are not backward compatible, use expand/contract migration phases.

Recommended pre-traffic checks:

```bash
PYTHONPATH=services/shared python scripts/validate_event_contracts.py
PYTHONPATH=services/shared python scripts/check_contract_compatibility.py
python scripts/schema_drift_report.py --pretty
python scripts/validate_privacy_catalog.py
```
