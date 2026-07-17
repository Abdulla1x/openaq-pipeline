"""Parse-time environment shared by all DagBag tests in this directory.

Must run before any DAG module is imported (pytest imports conftest first):

- Production Airflow adds DAGS_FOLDER to sys.path (module-management docs);
  replicated here so cross-DAG-file imports (openaq_datasets) resolve.
- The cosmos render of openaq_transform shells out to `dbt ls`, which needs
  the dbt project, the profile's env vars, and an existing credentials file
  path (never read for `ls` — an empty JSON stub suffices).
- cosmos's dbt-ls cache is disabled: it persists to an Airflow Variable and
  these tests run without a metadata DB. The real deployment keeps it.

No airflow import here — tests/dags must stay collectable (and skippable)
in the unit-test environments that don't install airflow.
"""

import os
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).parents[2]

sys.path.insert(0, str(REPO_ROOT / "airflow" / "dags"))

_fake_key = Path(tempfile.gettempdir()) / "openaq-dag-tests-fake-key.json"
_fake_key.write_text("{}", encoding="utf-8")

for var, value in {
    "GCP_PROJECT_ID": "test-project",
    "GCS_BUCKET_NAME": "test-bucket",
    "BIGQUERY_RAW_DATASET": "openaq_raw",
    "BIGQUERY_DATASET": "openaq_dbt",
    "GOOGLE_APPLICATION_CREDENTIALS": str(_fake_key),
    "DBT_PROJECT_DIR": str(REPO_ROOT / "dbt"),
    "DBT_PROFILES_DIR": str(REPO_ROOT / "dbt"),
    "AIRFLOW__COSMOS__ENABLE_CACHE": "false",
}.items():
    os.environ.setdefault(var, value)
