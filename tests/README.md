# tests/

Pytest test suite.

## Contents (current)

```
tests/
├── unit/
│   ├── test_who_constants.py   Guards the WHO 2021 threshold values (G5)
│   ├── test_who_seed_sync.py   dbt seed ↔ constants.py sync (incl. the O3 key mapping)
│   ├── test_client.py          OpenAQ client: auth, throttling, retries, pagination (mocked API)
│   ├── test_ingest.py          Fan-out: country resolution, sensor extraction, fault isolation,
│   │                           lookback/window fetch contracts
│   ├── test_backfill.py        Chunk math, checkpoint/resume, threshold + reconcile guards
│   └── test_gcs.py             Raw-zone writer: object naming (day + backfill partitions), NDJSON
├── dags/
│   ├── conftest.py             Parse-time env + DAGS_FOLDER on sys.path (as production does)
│   ├── test_dag_integrity.py   DagBag import + structure of the openaq_ingest DAG
│   ├── test_ingest_task_behavior.py   Failure model: 20% threshold + zero-sensor guard
│   └── test_dag_transform.py   Cosmos per-node tasks + the Dataset schedule contract (G9)
└── integration/
    └── test_live_roundtrip.py  One live API→GCS→BQ-load-contract round-trip (Phase 5)
```

Import paths are configured via `pythonpath = ["."]` in `pyproject.toml`
(`[tool.pytest.ini_options]`); `tests/dags/conftest.py` additionally sets the
parse-time environment the DAG modules and the cosmos render need.

## Running tests

```bash
pytest tests/unit/ -v    # 42 unit tests; fast, no credentials needed (also: make test)
make dag-test            # quick DAG import check inside the Airflow container
make integration-test    # ONE live round-trip; needs a sourced .env (never in CI)
```

The unit suite mocks the OpenAQ API with `responses` and injects the GCS
bucket handle — no network or credentials in CI (G12).

The DAG suite needs a real Airflow install, so it runs in its own
`dag-validate` CI job (mirroring the image's constraint-pinned install) and
skips cleanly in environments without Airflow.

The integration test hits live OpenAQ + GCS + BigQuery and is deliberately
absent from CI: CI carries no GCP credentials by design. It self-skips when
`.env` isn't sourced, lands only under an isolated `_integration/` prefix no
production load glob can match, and deletes what it lands.
