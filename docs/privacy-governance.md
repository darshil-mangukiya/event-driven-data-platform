# Privacy Governance

Privacy controls are represented as catalog, SQL, and runbook artifacts.

## Classification Catalog

`governance/pii_classification.json` classifies fields by dataset, sensitivity, handling requirement, and retention policy.

Validate it with:

```bash
python scripts/validate_privacy_catalog.py
```

## Retention

`data_retention_policies` stores table-level retention guidance. Local policies are seeded for raw events, API usage, service health, and quality results.

## Subject Erasure

`sql/privacy/tenant_erasure_plan.sql` documents a governed subject-erasure flow. It records a request in `privacy_erasure_requests`, masks/hash-replaces user-level identifiers where the platform can do so safely, and writes completion evidence.

Finance and audit data may require tokenization instead of deletion. Retention
and deletion policy must account for that distinction.

## API Masking Boundary

The analytics APIs expose aggregate metrics and tenant-scoped operational payloads. They should not expose raw user identifiers. If future endpoints include customer-level detail, they should return masked IDs by default and require explicit privileged scopes for re-identification.
