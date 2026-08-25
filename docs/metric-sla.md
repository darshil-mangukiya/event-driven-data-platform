# Metric SLA

| Metric/API | Freshness target | Availability target | Correctness control |
| --- | ---: | ---: | --- |
| `/metrics/revenue` | 15 minutes | 99.5% monthly | daily reconciliation and backfill |
| `/metrics/customers` | 30 minutes | 99.5% monthly | data quality checks |
| `/metrics/marketing_roi` | 60 minutes | 99.0% monthly | campaign attribution completeness checks |
| `/metrics/product_performance` | 60 minutes | 99.0% monthly | product metadata coverage |
| `/system/status` | 5 minutes | 99.9% monthly | health event freshness |

SLA exceptions:

- local Docker development
- planned backfills
- schema migrations
- incident mitigation windows

Production consumers should use this as a starting contract and negotiate stricter guarantees per business workflow.
