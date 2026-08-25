.PHONY: help setup doctor install test coverage coverage-report lint compile up down demo smoke load e2e migrate backup-postgres restore-postgres-dry-run backup-restore-drill quality benchmark-report benchmark-compare load-test-and-compare synthetic contracts-check contracts-compatibility metric-contracts catalog-check samples-check privacy-check schema-drift lifecycle-plan rls-check auth-posture schema-registry-validate asyncapi-generate asyncapi-validate k8s-cluster-up k8s-cluster-down k8s-build-images k8s-load-images k8s-deploy helm-sync-init-scripts helm-lint helm-template helm-deploy k8s-status terraform-validate rls-check-live ai-incident-copilot openapi-export lineage-dry-run incident-drill outbox-plan evidence-bundle preflight ci-local cli-health cli-config cli-tenant-dry-run cli-evidence reconciliation-dry-run reconciliation-all-dry-run reconciliation-summary-dry-run reconciliation-test resilience-probe backfill-dry-run ops-check dbt-run dbt-test streaming-test streaming-test-fast streaming-demo streaming-demo-down reliability-test reliability-report reliability-poison-event reliability-duplicate-event reliability-late-event reliability-consumer-lag reliability-db reliability-redis reliability-reconciliation reliability-consumer-interruption reliability-all data-products-list data-products-validate data-products-catalog requirements-trace data-products-test lineage-validate lineage-graph lineage-test

# Default target: `make` with no arguments shows the command list instead of
# silently running the first target in the file (which used to be `install`
# by file-order accident — surprising when the same Makefile has grown to
# developer and validation targets). See CONTRIBUTING.md for the full
# onboarding flow.
.DEFAULT_GOAL := help

help: ## Show this list of available make targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_-]+:.*?## / {printf "  \033[36m%-32s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

## --- Onboarding -------------------------------------------------------

setup: ## First-time local setup: copy .env, install deps, run doctor
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example — review it before starting Docker.")
	python -m pip install -r requirements.txt
	python scripts/dev_doctor.py

doctor: ## Diagnose your local machine (Python version, Docker, .env, port conflicts)
	python scripts/dev_doctor.py

## --- Install / test / lint ---------------------------------------------

install: ## Install Python dependencies
	python -m pip install -r requirements.txt

test: ## Run the full pytest suite
	PYTHONPATH=services/shared:services/processing-service pytest -q

coverage: ## Run pytest with coverage instrumentation (services/scripts/platform_cli/data_products/lineage/reliability)
	PYTHONPATH=.:services/shared python -m pytest --cov=services --cov=scripts --cov=platform_cli --cov=data_products --cov=lineage --cov=reliability --cov-report=term-missing --cov-report=html:evidence/coverage/htmlcov -q

coverage-report: ## Generate the checked-in coverage evidence report
	PYTHONPATH=.:services/shared python scripts/generate_coverage_report.py

lint: ## Run ruff
	ruff check --no-cache .

compile: ## Byte-compile all Python packages (fast syntax-error check)
	python -m compileall services/shared services/processing-service services/ingestion-service services/analytics-service services/metadata-service services/demo-dashboard scripts platform_cli examples kafka spark/jobs sdk/python database/migrations

ci-local: lint test compile contracts-check contracts-compatibility catalog-check samples-check privacy-check schema-drift metric-contracts rls-check lineage-validate data-products-validate cli-config cli-health ## Reproduce .github/workflows/ci.yml locally, in order, before you push
	docker compose -f docker-compose.yml config --quiet
	@echo "ci-local: all steps passed."

## --- Local stack --------------------------------------------------------

up: ## Start the full local Docker Compose stack
	docker compose -f docker-compose.yml up --build

down: ## Stop the local Docker Compose stack
	docker compose -f docker-compose.yml down

demo: ## Run scripts/demo_mode.py against the (already running) local stack
	PYTHONPATH=services/shared:services/processing-service python scripts/demo_mode.py

smoke: ## Run lightweight API smoke checks against the running stack
	python scripts/api_smoke_test.py

load: ## Run a small local load test against the ingestion API
	python scripts/load_test_events.py --batches 20 --batch-size 50

benchmark-report: ## Render docs/benchmark-evidence.md from the latest load test
	python scripts/benchmark_report.py --output docs/benchmark-evidence.md

benchmark-compare: ## Compare the most recent benchmarks/results/*.json against the checked-in baseline (fails on regression)
	@latest=$$(ls -t benchmarks/results/*.json 2>/dev/null | head -1); \
	if [ -z "$$latest" ]; then echo "No benchmark result files in benchmarks/results/ — run 'make load-test-and-compare' first."; exit 1; fi; \
	echo "Comparing $$latest against samples/benchmarks/local_ingestion_sample.json"; \
	python scripts/compare_benchmarks.py --current "$$latest" --pretty

load-test-and-compare: ## Run a real load test against the running local stack, then gate it against the baseline
	python scripts/load_test_events.py --batches 50 --batch-size 100 --output benchmarks/results/local-ingestion-$$(date +%Y%m%d%H%M%S).json
	python scripts/compare_benchmarks.py --current $(shell ls -t benchmarks/results/*.json 2>/dev/null | head -1) --pretty

e2e: ## Run the local end-to-end API flow
	python scripts/run_local_e2e.py

quality: ## Run data quality checks against the running stack
	PYTHONPATH=services/shared python scripts/run_data_quality_checks.py --pretty

synthetic: ## Post 1000 tenant-patterned local events to ingestion
	PYTHONPATH=services/shared python scripts/generate_synthetic_events_v2.py --count 1000

migrate: ## Apply Alembic migrations
	alembic upgrade head

backup-postgres: ## Take a pg_dump backup with a verifiable row-count manifest (requires pg_dump on PATH)
	python scripts/backup_postgres.py --pretty

restore-postgres-dry-run: ## Validate a backup's dump/manifest pair without touching any database (pass DUMP=path/to/file.dump)
	python scripts/restore_postgres.py --dump $(DUMP) --dry-run --pretty

backup-restore-drill: ## Full DR drill: backup -> restore into a scratch database -> verify row counts -> drop scratch db
	python scripts/backup_restore_drill.py --pretty

## --- Contracts / governance checks --------------------------------------

contracts-check: ## Validate event contracts
	PYTHONPATH=services/shared python scripts/validate_event_contracts.py

contracts-compatibility: ## Check event contract backward-compatibility
	PYTHONPATH=services/shared python scripts/check_contract_compatibility.py

metric-contracts: ## Validate metric contracts
	python scripts/validate_metric_contracts.py

catalog-check: ## Validate catalog/data_catalog.json
	python scripts/validate_catalog.py

samples-check: ## Validate sample artifacts against their contracts
	PYTHONPATH=services/shared python scripts/validate_sample_artifacts.py

privacy-check: ## Validate the privacy/PII catalog
	python scripts/validate_privacy_catalog.py

schema-drift: ## Report schema drift between models and the live schema
	python scripts/schema_drift_report.py --pretty

lifecycle-plan: ## Render the data lifecycle/retention plan
	python scripts/lifecycle_retention_plan.py --pretty

rls-check: ## Validate tenant row-level-security policies
	python scripts/validate_tenant_rls.py

rls-check-live: ## Run the real RLS runtime test matrix against a live database (requires database/security/tenant_rls.sql applied)
	python scripts/validate_tenant_rls.py --live --pretty

ai-incident-copilot: ## Run the offline AI Incident Copilot against a reliability exercise — e.g. `make ai-incident-copilot SCENARIO=db-outage`
	PYTHONPATH=.:services/shared python scripts/ai_incident_copilot_run.py --scenario $(or $(SCENARIO),db-outage) --pretty

auth-posture: ## Report the current AUTH_MODE / JWT_SECRET posture (see docs/security.md)
	PYTHONPATH=.:services/shared python scripts/validate_auth_posture.py --pretty

schema-registry-validate: ## Bootstrap + live-validate the runtime Schema Registry (requires `make up` schema-registry running)
	PYTHONPATH=.:services/shared python scripts/validate_schema_registry.py --all --pretty

asyncapi-generate: ## Regenerate contracts/asyncapi.yml from real topic/schema/registry configuration
	PYTHONPATH=.:services/shared python scripts/generate_asyncapi.py

asyncapi-validate: ## Cross-reference contracts/asyncapi.yml against real topics/schemas/event-types
	PYTHONPATH=.:services/shared python scripts/validate_asyncapi.py

k8s-cluster-up: ## Create the local kind cluster (cloudscale)
	kind create cluster --name cloudscale --wait 90s

k8s-cluster-down: ## Delete the local kind cluster
	kind delete cluster --name cloudscale

k8s-build-images: ## Build the 4 core service images tagged :local for kind
	docker build -f docker/Dockerfile.service --build-arg SERVICE_PATH=services/ingestion-service -t cloudscale-ingestion-service:local .
	docker build -f docker/Dockerfile.service --build-arg SERVICE_PATH=services/processing-service -t cloudscale-processing-service:local .
	docker build -f docker/Dockerfile.service --build-arg SERVICE_PATH=services/analytics-service -t cloudscale-analytics-service:local .
	docker build -f docker/Dockerfile.service --build-arg SERVICE_PATH=services/metadata-service -t cloudscale-metadata-service:local .

k8s-load-images: ## Load the built :local images into the kind cluster
	kind load docker-image cloudscale-ingestion-service:local --name cloudscale
	kind load docker-image cloudscale-processing-service:local --name cloudscale
	kind load docker-image cloudscale-analytics-service:local --name cloudscale
	kind load docker-image cloudscale-metadata-service:local --name cloudscale

k8s-deploy: ## Apply the raw manifests in deploy/kubernetes/base/ (kubectl apply path)
	kubectl apply -f deploy/kubernetes/base/00-namespace.yaml
	kubectl create configmap postgres-init-scripts --from-file=database/init/ -n cloudscale --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f deploy/kubernetes/base/

helm-sync-init-scripts: ## Re-sync executable Postgres initialization SQL into the Helm chart (see its README)
	cp database/init/001_schema.sql deploy/helm/cloudscale/files/init/001_schema.sql
	cp database/init/002_seed.sql deploy/helm/cloudscale/files/init/002_seed.sql
	cp database/init/003_local_demo_transactional_seed.sql deploy/helm/cloudscale/files/init/003_local_demo_transactional_seed.sql
	cp database/security/tenant_rls.sql deploy/helm/cloudscale/files/init/004_tenant_rls.sql

helm-lint: ## helm lint the cloudscale chart
	helm lint deploy/helm/cloudscale

helm-template: ## Render the cloudscale chart to stdout
	helm template cloudscale deploy/helm/cloudscale

helm-deploy: helm-sync-init-scripts ## Install/upgrade the platform from the packaged Helm chart
	helm upgrade --install cloudscale deploy/helm/cloudscale --create-namespace

k8s-status: ## Show pods/services/deployments/statefulsets in the cloudscale namespace
	kubectl get pods,svc,deployments,statefulsets -n cloudscale

terraform-validate: ## terraform fmt -check + validate the AWS target architecture (no apply, no AWS credentials needed)
	cd infra/aws/terraform && terraform init -backend=false -input=false && terraform fmt -check -diff && terraform validate

openapi-export: ## Export OpenAPI contracts for all FastAPI services
	PYTHONPATH=services/shared:services/processing-service python scripts/export_openapi_contracts.py

lineage-dry-run: ## Dry-run a lineage event emission
	PYTHONPATH=services/shared python scripts/emit_lineage_event.py --job-name backfill_tenant_metrics_daily --tenant-id tenant_demo --inputs processed_orders,processed_payments,processed_user_sessions --outputs tenant_metrics_daily --status succeeded --dry-run

incident-drill: ## Render an incident drill report
	python scripts/incident_drill.py --pretty

outbox-plan: ## Render the outbox dispatch plan
	python scripts/outbox_dispatch_plan.py

evidence-bundle: ## Generate the full evidence bundle
	python scripts/generate_evidence_bundle.py --pretty

preflight: ## Run release-readiness governance checks (scripts/platform_preflight.py)
	PYTHONPATH=services/shared:services/processing-service python scripts/platform_preflight.py --output-json evidence/validation/release-readiness.json --output-md evidence/validation/release-readiness.md --pretty

ops-check: contracts-check contracts-compatibility metric-contracts catalog-check samples-check privacy-check schema-drift rls-check resilience-probe ## Run the full contracts/governance/resilience check bundle

## --- Platform CLI ---------------------------------------------------------

cli-health: ## platform_cli health check (dry-run)
	PYTHONPATH=.:services/shared python -m platform_cli --pretty health check --dry-run

cli-config: ## platform_cli config validate
	PYTHONPATH=.:services/shared python -m platform_cli --pretty config validate

cli-tenant-dry-run: ## platform_cli tenant create (dry-run)
	PYTHONPATH=.:services/shared python -m platform_cli --pretty tenant create --tenant-id tenant_newco --tenant-name "NewCo Analytics" --dry-run

cli-evidence: ## platform_cli evidence generate
	PYTHONPATH=.:services/shared python -m platform_cli --pretty evidence generate --output-dir evidence/validation

## --- Reconciliation ---------------------------------------------------------

reconciliation-dry-run: ## Dry-run the revenue reconciliation check
	PYTHONPATH=services/shared python scripts/reconcile_metrics.py --tenant-id tenant_demo --start-date 2026-05-01 --end-date 2026-05-07 --dry-run --pretty

reconciliation-all-dry-run: ## Dry-run all three reconciliation checks
	PYTHONPATH=.:services/shared python scripts/reconcile_metrics.py --tenant-id tenant_demo --start-date 2026-05-01 --end-date 2026-05-07 --check all --dry-run --pretty

reconciliation-summary-dry-run: ## Dry-run the reconciliation summary report
	PYTHONPATH=services/shared python scripts/reconciliation_summary.py --tenant-id tenant_demo --days 7 --dry-run --pretty

reconciliation-test: ## Run reconciliation tests only
	PYTHONPATH=.:services/shared python -m pytest tests/test_reconciliation.py -v

resilience-probe: ## Dry-run the resilience probe
	python scripts/resilience_probe.py --dry-run

backfill-dry-run: ## Dry-run the tenant_metrics_daily backfill
	PYTHONPATH=services/shared python scripts/backfill_metrics.py --tenant-id tenant_demo --start-date 2026-05-01 --end-date 2026-05-07 --dry-run --pretty

## --- dbt --------------------------------------------------------------

dbt-run: ## Run dbt models
	cd dbt && dbt run --profiles-dir .

dbt-test: ## Run dbt tests
	cd dbt && dbt test --profiles-dir .

## --- Structured Streaming (spark/streaming/) ----------------------------
# Requires a local JDK (e.g. `brew install openjdk@17`) and pyspark
# installed (see requirements.txt). Fast unit tests run against
# static/batch DataFrames; the "integration" marker runs real live
# streaming queries (rate source, real checkpoints).

streaming-test: ## Run all Structured Streaming tests (incl. live integration)
	PYTHONPATH=. python -m pytest tests/streaming -q

streaming-test-fast: ## Run Structured Streaming unit tests only (no live queries)
	PYTHONPATH=. python -m pytest tests/streaming -q -m "not integration"

streaming-demo: ## Run the Structured Streaming job against the dockerized stack (requires `make up`)
	docker compose -f docker-compose.yml --profile streaming up --build spark-streaming

streaming-demo-down: ## Stop the Structured Streaming demo
	docker compose -f docker-compose.yml --profile streaming down

## --- Reliability exercises (reliability/scenarios/) ----------------------
# Each one runs real code paths — some need live Kafka/PostgreSQL/Redis/
# Docker and report "not_run" when that infrastructure isn't reachable;
# others (poison-event, duplicate-event, late-event, redis-outage,
# db-outage, reconciliation-mismatch, consumer-interruption) exercise the
# platform's real validation/dedup/watermark/cache/sink/reconciliation code
# deterministically, without needing any of that infra. See docs/reliability.md.

reliability-test: ## Run reliability exercise tests
	PYTHONPATH=.:services/shared python -m pytest tests/reliability -q

reliability-report: reliability-all ## Run every reliability exercise and point at the generated evidence
	@echo "Evidence written under artifacts/reliability/<run_id>/ — see evidence/validation/ for the summary."

reliability-poison-event: ## Run the poison-event reliability exercise
	PYTHONPATH=.:services/shared python -m platform_cli --pretty reliability run poison-event

reliability-duplicate-event: ## Run the duplicate-event reliability exercise
	PYTHONPATH=.:services/shared python -m platform_cli --pretty reliability run duplicate-event

reliability-late-event: ## Run the late-event reliability exercise
	PYTHONPATH=.:services/shared python -m platform_cli --pretty reliability run late-event

reliability-consumer-lag: ## Run the consumer-lag reliability exercise
	PYTHONPATH=.:services/shared python -m platform_cli --pretty reliability run consumer-lag

reliability-db: ## Run the db-outage reliability exercise
	PYTHONPATH=.:services/shared python -m platform_cli --pretty reliability run db-outage

reliability-redis: ## Run the redis-outage reliability exercise
	PYTHONPATH=.:services/shared python -m platform_cli --pretty reliability run redis-outage

reliability-reconciliation: ## Run the reconciliation-mismatch reliability exercise
	PYTHONPATH=.:services/shared python -m platform_cli --pretty reliability run reconciliation-mismatch

reliability-consumer-interruption: ## Run the consumer-interruption reliability exercise
	PYTHONPATH=.:services/shared python -m platform_cli --pretty reliability run consumer-interruption

reliability-all: ## Run every reliability exercise
	PYTHONPATH=.:services/shared python -m platform_cli --pretty reliability run --all

## --- Data products / lineage --------------------------------------------

data-products-list: ## List the data product registry via platform_cli
	PYTHONPATH=.:services/shared python -m platform_cli --pretty data-products list

data-products-validate: ## Validate contracts/data_products/registry.yml
	PYTHONPATH=.:services/shared python scripts/validate_data_products.py

data-products-catalog: ## Render the data product catalog report
	PYTHONPATH=.:services/shared python scripts/generate_data_product_catalog.py

requirements-trace: ## Alias for data-products-catalog (consumer requirements traceability)
	PYTHONPATH=.:services/shared python scripts/generate_data_product_catalog.py

data-products-test: ## Run data product tests only
	PYTHONPATH=.:services/shared python -m pytest tests/test_data_products.py -v

lineage-validate: ## Validate the lineage graph (cycles/orphans/cross-reference)
	PYTHONPATH=. python scripts/validate_lineage.py

lineage-graph: ## Render the lineage report
	PYTHONPATH=. python scripts/generate_lineage_report.py

lineage-test: ## Run lineage tests only
	PYTHONPATH=.:services/shared python -m pytest tests/test_lineage.py -v
