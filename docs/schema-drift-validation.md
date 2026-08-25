# Schema Drift Validation

The schema drift report compares:

- `database/init/001_schema.sql`
- `catalog/data_catalog.json`
- Alembic migration revisions

Run:

```bash
python scripts/schema_drift_report.py --pretty
```

The report fails when the catalog references a table or view not present in the SQL schema. It also surfaces uncataloged core objects so reviewers can decide whether the data catalog should be expanded.

Production extension:

- compare live `information_schema` against migrations
- run migration smoke tests in ephemeral databases
- require catalog updates for new serving tables
- add schema drift output to deployment gates
