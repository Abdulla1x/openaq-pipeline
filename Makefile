.PHONY: up down logs lint test dag-test freshness elementary-bootstrap elementary-report integration-test

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

lint:
	ruff check .

test:
	pytest tests/unit/ -v

# dbt source freshness against ingested_at (thresholds in sources.yml).
# Runs in the Airflow image — that's where the pinned dbt 1.8.3 lives.
freshness:
	docker compose run --rm --no-deps webserver bash -c \
		"cd /opt/airflow/dbt && dbt source freshness"

# One-time creation of the elementary package's metadata tables in
# openaq_dbt_elementary (Terraform-provisioned). Re-running is safe.
elementary-bootstrap:
	docker compose run --rm --no-deps webserver bash -c \
		"cd /opt/airflow/dbt && dbt run --select elementary"

# Generates elementary_report.html (gitignored) from the metadata tables.
# Needs the dedicated edr venv (its dep tree conflicts with both the pinned
# dev venv and the Airflow image): see tests/README.md / docs.
elementary-report:
	.venv-edr/bin/edr report --project-dir dbt --profiles-dir dbt \
		--file-path elementary_report.html

# Live round-trip test (API -> GCS -> BQ load contract); needs a sourced .env
# with real credentials. Deliberately NOT in CI (no credentials there).
integration-test:
	pytest tests/integration/ -m integration -v

# Quick in-container check that every DAG imports cleanly (the full
# structural tests run in the dag-validate CI job).
dag-test:
	docker compose run --rm webserver python -c "import sys; \
	from airflow.models import DagBag; \
	db = DagBag('/opt/airflow/dags', include_examples=False); \
	sys.exit('import errors: %s' % db.import_errors if db.import_errors else 0)"
