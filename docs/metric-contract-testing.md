# Metric Contract Testing

Metric contracts live in `metrics/contracts/`.

Validate contracts:

```bash
python scripts/validate_metric_contracts.py
```

Validate a fixture against a metric contract:

```bash
python scripts/validate_metric_contracts.py \
  --metric revenue \
  --fixture api/fixtures/analytics_revenue_response.json
```

The contract checks required response fields, non-negative fields, and tenant-scoped metric declaration.
