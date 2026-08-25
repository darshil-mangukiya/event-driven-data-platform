# Cloud Environment Matrix

| Environment | Purpose | Data | Scale | Controls |
| --- | --- | --- | --- | --- |
| local | development and local demo | initialized local dataset | laptop | Docker Compose |
| dev | shared engineering validation | non-production/scrubbed | small | CI checks, ephemeral data |
| staging | release rehearsal | scrubbed production-like | medium | migrations, preflight, smoke/load tests |
| production | business workloads | governed tenant data | autoscaled | SLOs, DR, privacy workflows, alerting |

Promotion gates:

- contract compatibility
- schema drift report
- metric contracts
- RLS policy validation
- privacy catalog validation
- smoke/load tests
