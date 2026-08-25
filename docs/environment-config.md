# Environment-Aware Configuration

The shared `Settings` object reads service configuration from environment variables and validates local, test, staging, and production-style values.

## Environments

| Environment | Purpose |
| --- | --- |
| `local` | Docker Compose development stack. |
| `test` | Unit/contract test runs. |
| `staging` | Production-like checks before deployment. |
| `production` | Managed infrastructure and hardened secrets. |

## Validation

```bash
PYTHONPATH=.:services/shared python -m platform_cli --pretty config validate
```

The validator checks:

- environment name
- PostgreSQL URL shape
- Redis URL shape
- Kafka bootstrap configuration
- positive cache TTL
- positive rate limit
- staging/production localhost warnings

## Production Notes

- Do not use local placeholder secrets.
- Use managed Kafka/PostgreSQL/Redis endpoints.
- Store JWT secrets and database credentials in a secrets manager.
- Keep `.env.example` safe and illustrative only.
