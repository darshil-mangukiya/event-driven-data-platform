# Testing Strategy

The repository uses one pytest suite for unit, contract, governance, Spark,
and optional live-service checks.

Current collection: **475 tests**. The final local run completed with
**466 passed, 9 skipped, 0 failed**.

## Organization

```text
tests/                 Unit, API, contract, governance, and tooling tests
tests/streaming/       Spark Structured Streaming tests
tests/reliability/     Reliability scenario tests
tests/integration/     Event-processing flow tests
```

Run the full suite:

```bash
PYTHONPATH=.:services/shared python -m pytest -q
```

Run without integration-marked tests:

```bash
PYTHONPATH=.:services/shared python -m pytest -m "not integration" -q
```

Tests that need PostgreSQL, Keycloak, or the schema-registry service check
reachability and skip when the dependency is absent. Spark workers must use the
same Python minor version as the pytest driver:

```bash
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python" \
PYTHONPATH=.:services/shared python -m pytest -q
```

## Test doubles

Async database tests use small `FakePostgres` implementations or
`AsyncMock` objects. They record SQL and positional parameters so tests can
check table selection, tenant predicates, and parameter ordering without I/O.
Live PostgreSQL tests cover pool behavior, RLS, seed consistency, and
backup/restore.

Several services use the package name `app`. Tests that import more than one
service should load modules with `importlib.util.spec_from_file_location` or
defer the import until test execution to avoid `sys.modules["app"]`
collisions.

## Coverage

```bash
make coverage
make coverage-report
```

The coverage run includes services, scripts, CLI, data products, lineage, and
reliability modules. Spark jobs are measured by the streaming tests because
JVM workers do not combine cleanly with single-process coverage
instrumentation.

The generated summary is
`evidence/validation/test-coverage-report.md`. Low-coverage CLI wrappers and
Kafka/Docker integration paths require their runtime dependencies; core helper
logic is tested independently.

## Validation layers

- Ruff and Python compilation catch static errors.
- Contract, catalog, privacy, lineage, AsyncAPI, metric, and RLS validators run
  in CI.
- Compose, Helm, Kubernetes YAML, and Terraform are validated separately.
- Live-service checks supplement the default suite when their dependencies are
  running.

See `../evidence/README.md` for retained runtime records.
