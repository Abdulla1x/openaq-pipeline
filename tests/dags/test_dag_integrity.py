"""DAG integrity: the openaq_ingest DAG must parse and keep its contract.

Runs where apache-airflow is installed — the `dag-validate` CI job or the
Airflow container (`make dag-test`) — and skips elsewhere, so the fast
unit-test environments don't need the airflow dependency tree.
"""

from pathlib import Path

import pytest

# Skip on airflow.models, not "airflow": the repo's airflow/ directory is a
# namespace package on pytest's sys.path, so bare "airflow" always imports.
pytest.importorskip("airflow.models", reason="requires apache-airflow (dag-validate job)")

from airflow.models import DagBag  # noqa: E402
from airflow.models.mappedoperator import MappedOperator  # noqa: E402

DAGS_DIR = Path(__file__).parents[2] / "airflow" / "dags"

EXPECTED_TASK_IDS = {
    "ingest_ae.prepare_country_run",
    "ingest_ae.fetch_sensor",
    "ingest_ae.summarize_country",
    "ingest_pk.prepare_country_run",
    "ingest_pk.fetch_sensor",
    "ingest_pk.summarize_country",
    "ensure_raw_table",
    "load_raw_to_bq",
    "reconcile_counts",
}


@pytest.fixture(scope="module")
def ingest_dag():
    # Parse-time env vars and sys.path come from conftest.py (shared with
    # test_dag_transform, and needed before DagBag imports any DAG module).
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"
    # dagbag.dags, not get_dag(): the latter consults the metadata DB, which
    # doesn't exist in this test environment.
    assert "openaq_ingest" in dagbag.dags
    return dagbag.dags["openaq_ingest"]


def test_structure(ingest_dag):
    assert {t.task_id for t in ingest_dag.tasks} == EXPECTED_TASK_IDS
    assert ingest_dag.catchup is False
    assert ingest_dag.max_active_runs == 1


def test_fetch_is_dynamically_mapped_behind_the_api_pool(ingest_dag):
    for country in ("ae", "pk"):
        fetch = ingest_dag.get_task(f"ingest_{country}.fetch_sensor")
        assert isinstance(fetch, MappedOperator), "G3: fetch must be a mapped task"
        assert fetch.pool == "openaq_api"


def test_load_emits_the_raw_measurements_dataset(ingest_dag):
    load = ingest_dag.get_task("load_raw_to_bq")
    assert [d.uri for d in load.outlets] == [
        "bigquery://test-project/openaq_raw/raw_measurements"
    ]


def test_load_runs_after_both_countries_and_before_reconcile(ingest_dag):
    ensure = ingest_dag.get_task("ensure_raw_table")
    assert ensure.upstream_task_ids == {
        "ingest_ae.summarize_country",
        "ingest_pk.summarize_country",
    }
    assert ingest_dag.get_task("load_raw_to_bq").downstream_task_ids == {"reconcile_counts"}


def test_load_reads_both_country_prefixes_verbatim(ingest_dag):
    load = ingest_dag.get_task("load_raw_to_bq")
    uris = load.configuration["query"]["tableDefinitions"]["raw_lines"]["sourceUris"]
    # Must not end in .json: template_ext would make Airflow load the URI as
    # a template file at render time (TemplateNotFound — seen live).
    assert uris == [
        "gs://test-bucket/raw/openaq/AE/{{ ds }}/*",
        "gs://test-bucket/raw/openaq/PK/{{ ds }}/*",
    ]
