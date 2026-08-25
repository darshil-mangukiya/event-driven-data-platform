# Contributing

This is a local-first platform: the whole point is that you can run
everything — services, streaming, reliability drills, reconciliation — on
your own machine without any cloud account. This doc is the fast path to a
working local setup and a green `make ci-local` before you push.

## First-time setup

```bash
python -m venv .venv-upgrade
source .venv-upgrade/bin/activate
make setup
```

`make setup` copies `.env.example` to `.env` if you don't already have one
(it never overwrites an existing `.env`), installs `requirements.txt`, and
then runs `make doctor` to tell you if anything about your machine needs
attention before you start the stack.

Re-run the diagnostic on its own any time with:

```bash
make doctor
```

It checks your Python version, whether you're in a virtual environment,
whether `.env` exists, whether core dependencies import cleanly, whether
Docker is installed and its daemon reachable, and — the most common local
failure mode in this project — whether the host ports `docker-compose.yml`
wants (5432, 6379, 9092, 9090, 3000, 9001, 8001, 8003–8006, ...) are already
taken by something else on your machine. If a native, non-Docker PostgreSQL
is already using port 5432 (common on macOS), `make doctor` tells you the
exact fix already built into `docker-compose.yml`:

```bash
POSTGRES_HOST_PORT=15432 docker compose up -d postgres
```

`scripts/dev_doctor.py` is deliberately a different tool from
`scripts/platform_preflight.py` (`make preflight`): preflight checks
whether the *repo's own data/contracts/governance state* is release-ready;
doctor checks whether *your machine* can run the repo at all. Run doctor
first — a broken local environment will make every other check fail for
uninteresting reasons.

## Finding the command you want

This repo has around 70 `make` targets covering the local stack, contracts
and governance checks, reconciliation, reliability drills, streaming, data
products, and lineage. Run:

```bash
make help
```

to list all of them with a one-line description each. `make` with no
arguments does the same thing (`help` is the default goal). Every target in
the Makefile carries a `## description` comment for this reason — if you add
a new target, add the comment too, or it silently disappears from
`make help` (and `tests/test_dev_doctor.py` will fail the build to catch
exactly that).

## Before you push: reproducing CI locally

```bash
make ci-local
```

This runs the same checks `.github/workflows/ci.yml` runs — lint, the full
test suite, a compile pass, all contract/governance validators (event
contracts, contract compatibility, catalog, samples, privacy, schema drift,
metric contracts, RLS, lineage, data products), and the platform CLI smoke
checks — in the same order, so a failure here is a failure in CI too. It
does not build the Docker service image (that step is comparatively slow and
CI-specific); everything else matches.

If you only need one piece, `make help` will show you the individual
target — e.g. `make lint`, `make test`, `make lineage-validate`.

## Running the full local stack

```bash
docker compose config --quiet   # validate compose syntax first
make up                          # or: docker compose up --build
```

See [README.md](README.md#local-setup) for service URLs, and
[docs/demo-mode.md](docs/demo-mode.md) for the one-command `make demo` flow.
A fresh `docker compose up` initializes transactional development data automatically
(`database/init/003_local_demo_transactional_seed.sql` — see
[docs/local-data-generation.md](docs/local-data-generation.md)), so the demo dashboard and
reconciliation checks have populated local inputs immediately, with no manual steps.

## Tests

```bash
make test          # full suite
make coverage       # with coverage instrumentation
```

Tests that need live infrastructure (Docker Postgres/Redis/Kafka, a running
Spark session) are marked `@pytest.mark.integration` and skip cleanly with
`pytest.skip()` when that infra isn't reachable — see
`reliability/injectors/reachability.py` for the reachability probes they use,
and [docs/testing-strategy.md](docs/testing-strategy.md) for the full
testing approach. `tests/streaming/` needs a local JDK and pyspark; see
`make streaming-test-fast` for the subset that doesn't run live queries.

## Code style

```bash
make lint
```

Ruff is the only linter/formatter config (`pyproject.toml`'s `[tool.ruff]`).
There is no separate formatter step — `ruff check` covers import ordering
(`I`), pyflakes (`F`), pycodestyle (`E`), bugbear (`B`), and pyupgrade
(`UP`) rules already.

## Constraints worth knowing before you start

- Never introduce Snowflake, Tableau, GA4, BigQuery, SQL Server, Databricks,
  or Salesforce — the stack is intentionally Kafka + Spark Structured
  Streaming + FastAPI + PostgreSQL + Redis, all runnable locally.
- See [docs/LIMITATIONS.md](docs/LIMITATIONS.md) for what each subsystem
  does not cover yet, scoped per capability — check there
  before assuming something is a bug rather than a documented, honest gap.
