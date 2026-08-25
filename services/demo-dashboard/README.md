# Demo Dashboard

The demo dashboard is a lightweight FastAPI service for local walkthroughs. It reads PostgreSQL serving and observability tables and renders:

- Tenant KPI cards.
- Daily metrics.
- Alerts.
- Pipeline runs.
- Service health.
- Benchmark evidence.

Local URL after Docker Compose starts:

```text
http://localhost:8005/?tenant_id=tenant_demo
```
