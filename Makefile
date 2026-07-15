.PHONY: up down logs lint test dag-test

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

# Quick in-container check that every DAG imports cleanly (the full
# structural tests run in the dag-validate CI job).
dag-test:
	docker compose run --rm webserver python -c "import sys; \
	from airflow.models import DagBag; \
	db = DagBag('/opt/airflow/dags', include_examples=False); \
	sys.exit('import errors: %s' % db.import_errors if db.import_errors else 0)"
