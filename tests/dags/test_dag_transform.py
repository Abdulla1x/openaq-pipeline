"""DAG integrity: the cosmos-rendered transform DAG must keep its contract.

Same environment rules as test_dag_integrity (see conftest.py): runs in the
dag-validate CI job or the Airflow container, skips where airflow isn't
installed.
"""

import os
from pathlib import Path

import pytest

pytest.importorskip("airflow.models", reason="requires apache-airflow (dag-validate job)")

from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).parents[2] / "airflow" / "dags"

# G9 / Phase 4 exit criterion: one task per dbt node (seed, model, test) —
# never an opaque `dbt build`. TestBehavior.AFTER_EACH pairs each model with
# its tests so failures gate downstream models.
EXPECTED_TASK_IDS = {
    "who_thresholds.seed",
    "who_thresholds.test",
    "stg_measurements.run",
    "stg_measurements.test",
    "stg_locations.run",
    "stg_locations.test",
    "stg_sensors.run",
    "stg_sensors.test",
    "int_daily_aqi.run",
    "int_daily_aqi.test",
    "mart_country_compare.run",
    "mart_country_compare.test",
    "mart_annual_compare.run",
    "mart_annual_compare.test",
}


@pytest.fixture(scope="module")
def dagbag():
    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert bag.import_errors == {}, f"DAG import errors: {bag.import_errors}"
    return bag


@pytest.fixture(scope="module")
def transform_dag(dagbag):
    assert "openaq_transform" in dagbag.dags
    return dagbag.dags["openaq_transform"]


def test_cosmos_renders_one_task_per_dbt_node(transform_dag):
    assert {t.task_id for t in transform_dag.tasks} == EXPECTED_TASK_IDS


def test_schedule_is_exactly_the_ingest_dags_outlet(dagbag, transform_dag):
    """The G9 contract: transform runs when (and only when) ingest's load
    emits the raw_measurements Dataset. Both sides import openaq_datasets,
    so this guards against either file defining its own drifting URI."""
    expected_uri = "bigquery://{}/{}/raw_measurements".format(
        os.environ["GCP_PROJECT_ID"], os.environ["BIGQUERY_RAW_DATASET"]
    )
    triggers = [uri for uri, _ in transform_dag.dataset_triggers.iter_datasets()]
    ingest_load = dagbag.dags["openaq_ingest"].get_task("load_raw_to_bq")
    assert triggers == [expected_uri]
    assert [d.uri for d in ingest_load.outlets] == [expected_uri]


def test_dbt_tests_gate_downstream_models(transform_dag):
    assert transform_dag.get_task("int_daily_aqi.run").upstream_task_ids == {
        "stg_measurements.test",
        "stg_locations.test",
        "stg_sensors.test",
    }
    for mart in ("mart_country_compare", "mart_annual_compare"):
        assert transform_dag.get_task(f"{mart}.run").upstream_task_ids == {
            "int_daily_aqi.test",
            "who_thresholds.test",
        }


def test_dag_config(transform_dag):
    assert transform_dag.catchup is False
    assert transform_dag.max_active_runs == 1
