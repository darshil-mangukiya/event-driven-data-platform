# Data Lifecycle Automation

Lifecycle automation is represented by retention policies, a dry-run planner, and privacy erasure SQL.

Dry-run retention plan:

```bash
python scripts/lifecycle_retention_plan.py --pretty
```

The planner reads `governance/pii_classification.json` and emits candidate archive/delete/mask actions for fields with day-based retention policies.

Production extension:

- execute lifecycle actions through an approved scheduler
- archive raw data to object storage before deletion
- log every lifecycle action to an audit table
- require approval workflow for restricted PII erasure
