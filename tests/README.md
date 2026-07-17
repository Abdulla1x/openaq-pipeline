# tests/

Pytest test suite.

## Contents (current)

```
tests/
├── unit/
│   ├── test_who_constants.py   Guards the WHO 2021 threshold values (G5)
│   ├── test_who_seed_sync.py   dbt seed ↔ constants.py sync (incl. the O3 key mapping)
│   ├── test_client.py          OpenAQ client: auth, throttling, retries, pagination (mocked API)
│   ├── test_ingest.py          Fan-out: country resolution, sensor extraction, fault isolation
│   └── test_gcs.py             Raw-zone writer: object naming, verbatim NDJSON contract
├── dags/
│   ├── conftest.py             Parse-time env + DAGS_FOLDER on sys.path (as production does)
│   ├── test_dag_integrity.py   DagBag import + structure of the openaq_ingest DAG
│   └── test_dag_transform.py   Cosmos per-node tasks + the Dataset schedule contract (G9)
└── integration/                Empty until Phase 5
```

Import paths are configured via `pythonpath = ["."]` in `pyproject.toml`
(`[tool.pytest.ini_options]`); `tests/dags/conftest.py` additionally sets the
parse-time environment the DAG modules and the cosmos render need.

## Running tests

```bash
pytest tests/unit/ -v    # 27 unit tests; fast, no credentials needed (also: make test)
make dag-test            # quick DAG import check inside the Airflow container
```

The unit suite mocks the OpenAQ API with `responses` and injects the GCS
bucket handle — no network or credentials in CI (G12).

The DAG suite needs a real Airflow install, so it runs in its own
`dag-validate` CI job (mirroring the image's constraint-pinned install) and
skips cleanly in environments without Airflow. Phase 5 adds an integration
test against real GCS/BigQuery.
