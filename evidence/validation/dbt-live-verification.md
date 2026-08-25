# dbt Live Verification

Scope: Local verification
Date: 2026-08-22

## Status: LOCALLY VERIFIED

Status changed from `EXECUTED - PARTIAL` (parse-only;
`dbt compile` blocked at the database-connection stage because no local
Postgres was running) to **LOCALLY VERIFIED**: a full `dbt build` ran
successfully against a real local PostgreSQL instance seeded with real
demo data, with every model built and every test passing.

## Environment

- **Database target**: `postgresql://platform:[REDACTED]@127.0.0.1:15432/data_platform`
  (the project's own Docker Compose `postgres` service, started with
  `POSTGRES_HOST_PORT=15432` — see "A runtime defect found" below for why).
- **dbt**: `dbt-core` 1.11.8, `postgres` adapter 1.10.0 (system Python;
  not installed in `.venv`).
- **Seed data**: the repository's own demo seed (`database/init/003_local_demo_transactional_seed.sql`),
  applied automatically on container init — 3 tenants, 880 orders, 880
  payments, 6 `tenant_metrics_daily` rows.
- **RLS security SQL applied manually**: `database/security/tenant_rls.sql`
  is **not** part of the automatic `docker-entrypoint-initdb.d` init
  sequence (only `database/init/*.sql` is mounted there) — it was applied
  by hand verification via `psql ... < database/security/tenant_rls.sql`
  before running dbt, so the `platform_tenant_scoped`/`platform_admin_bypass`
  roles existed. This matches how RLS has been live-verified in prior
  sessions; documented here so it isn't mistaken for an automatic step.

## A runtime defect found: a local PostgreSQL port conflict

`dbt build` initially failed with `FATAL: role "platform" does not exist`
against `localhost:5432` — **not a dbt or project defect**. Diagnosis
(`lsof -nP -iTCP:5432 -sTCP:LISTEN`) found a **native Homebrew PostgreSQL
instance already listening on `127.0.0.1:5432`** on this machine,
alongside Docker Compose's own port-published Postgres. macOS's
most-specific-address-wins socket binding means connections to
`127.0.0.1`/`localhost` reached the native instance (which has no
`platform` role), not the Docker container, even though `docker compose
ps` showed the container's port mapping as healthy. Fixed by using the
project's own already-documented workaround (`scripts/dev_doctor.py`,
`docs/local-data-generation.md`): `POSTGRES_HOST_PORT=15432 docker compose up -d
postgres` to publish the container on a conflict-free port. This is a
genuine local-machine environment characteristic, not a code change —
nothing in the repository was altered to fix it.

## Commands run

```
$ docker compose up -d postgres redis kafka zookeeper schema-registry
$ POSTGRES_HOST_PORT=15432 docker compose up -d postgres   # port-conflict fix
$ psql -h 127.0.0.1 -p 15432 -U platform -d data_platform < database/security/tenant_rls.sql
$ cd dbt
$ POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=15432 DBT_PROFILES_DIR=. \
    dbt build --target-path /tmp/p6-dbt-build-target --log-path /tmp/p6-dbt-logs2
```

## Result

```
Found 7 models, 10 data tests, 6 sources, 4 metrics, 581 macros, 1 semantic model
Finished running 3 table models, 10 data tests, 4 view models in 0.46s
Completed successfully
Done. PASS=17 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=17
```

| Node | Type | Status | Duration |
|---|---|---|---|
| `stg_orders` | view | success | 0.088s |
| `stg_payments` | view | success | 0.090s |
| `stg_user_sessions` | view | success | 0.059s |
| `metricflow_time_spine` | view | success | 0.090s |
| `dim_tenants` | table | success | 0.090s |
| `fct_tenant_daily_metrics` | table | success | 0.029s |
| `fct_product_performance` | table | success | 0.032s |
| 10 data tests (`not_null` ×6, `accepted_values` ×1, `dbt_utils.accepted_range` ×1, plus 2 more `not_null` on the mart) | test | **all 10 pass** | 0.015–0.044s each |

## Generated artifacts (this run)

`/tmp/p6-dbt-build-target/manifest.json` (689,592 bytes), `run_results.json`
(18,767 bytes), `graph_summary.json`, `semantic_manifest.json`,
`compiled/` — written outside the repository (`--target-path`) to avoid
polluting the checked-in, gitignored `dbt/target/`. Not copied into
`evidence/` verbatim (large, machine-generated, and fully reproducible by
re-running the command above); this document summarizes their content.

## Actual relations created in PostgreSQL (verified via direct SQL)

```sql
\dn
--  analytics | analytics_mart | analytics_staging | public

select * from analytics_mart.dim_tenants;
--  tenant_demo | Demo Commerce  | growth     | us | t | {...}
--  tenant_enterprise | Northstar SaaS | enterprise | us | t | {...}
--  tenant_marketplace | MarketHub | growth | eu | t | {...}

select tenant_id, metric_date, net_revenue, payment_failure_rate, churn_signal_rate
from analytics_mart.fct_tenant_daily_metrics;
--  tenant_enterprise  | 2026-08-21 | 66973.54 | 0.0333 | 0.0077
--  tenant_marketplace | 2026-08-21 | 28779.45 | 0.0450 | 0.0280
--  tenant_demo        | 2026-08-21 | 35291.16 | 0.0333 | 0.0250
```

3 tenants in `dim_tenants`, 6 rows in `fct_tenant_daily_metrics` (matching
the seed's 6 `tenant_metrics_daily` source rows exactly), 8 rows in
`fct_product_performance` — all real, non-zero, NULLIF-guarded computed
values, not placeholders.

## Limitations

- Run against a single local demo dataset (3 tenants, one date,
  2026-08-21) — not a large-scale or multi-day dbt build.
- `dbt-core` runs from the system Python environment (Anaconda), not
  `.venv` — `dbt-core` and the `postgres` adapter are not listed
  in `requirements.txt`/`requirements.lock` (those cover the FastAPI
  services' runtime, not the analytics-engineering tooling's own
  environment). This is a pre-existing project characteristic, not
  introduced verification.
- No `dbt docs generate`/`dbt docs serve` was exercised.
- No incremental-model behavior was exercised — all 3 mart models are
  full-refresh `table` materializations; there is no incremental model in
  this project to test incremental-build idempotency for.
- Semantic-layer metrics (`tenant_net_revenue`, etc.) were confirmed to
  parse and resolve during `dbt build`'s manifest compilation, but no
  separate `mf query`/metric-value assertion was run against them this
  pass — the underlying `fct_tenant_daily_metrics` values they aggregate
  are directly verified above instead.

## Current status

This supersedes the earlier review's `CLM-0008`
(`EXECUTED - PARTIAL`, "dbt compile blocked at the DB-connection stage")
and `CLM-0023`/`CLM-0025` classifications — dbt is now **LOCALLY
VERIFIED**, not parse-only. See `REQUIRED_ENGINEERING_PASS_ADDENDUM.md`
for the full reconciliation against every claim this affects.
