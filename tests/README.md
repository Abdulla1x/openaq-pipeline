# tests/

Pytest test suite.

## Contents (current)

```
tests/
├── unit/
│   └── test_who_constants.py   Guards the WHO 2021 threshold values (G5)
└── integration/                Empty until Phase 5
```

Import paths are configured via `pythonpath = ["."]` in `pyproject.toml`
(`[tool.pytest.ini_options]`) — no conftest.py or editable install needed.

## Running tests

```bash
pytest tests/unit/ -v    # fast, no credentials needed (also: make test)
```

Phase 2 adds unit tests for the OpenAQ client (mocked API, per G12); Phase 5
adds an integration test against real GCS/BigQuery.
