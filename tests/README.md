# tests/

Pytest test suite for the Python ingestion layer.

## Contents

```
tests/
├── conftest.py       Shared fixtures (mock GCS client, sample API responses)
├── unit/             Pure unit tests — no network or GCP calls
└── integration/      End-to-end tests against real GCS/BigQuery (needs .env)
```

## Running tests

```bash
pytest tests/unit/                  # fast, no credentials needed
pytest tests/integration/ --slow    # requires GOOGLE_APPLICATION_CREDENTIALS
```
