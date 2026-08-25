# Migration Scaffold

Local Docker still runs `database/init/*.sql` for a clean developer experience. The Alembic scaffold gives the project a production migration path:

```bash
alembic upgrade head
```

`0001_initial_platform_schema.py` executes the existing schema SQL so local bootstrap and migration-based deployment use the same source of truth.

