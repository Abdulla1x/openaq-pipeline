"""Daily OpenAQ → GCS → BigQuery raw ingestion (Phase 3).

Design notes (guardrails in docs/PROJECT_CONTEXT.md):

- G2/G3 — the v3 API is sensor-centric, so the fetch fans out with **dynamic
  task mapping**: one mapped task per target sensor, discovered at runtime
  from the country's locations inventory.
- Failure model: **catch + threshold.** Individual sensor failures are data,
  not DAG failures — ~30 PK sensors persistently 500 server-side, so a red
  task per broken sensor would fail every run and make "failed DAG"
  meaningless. A country fails only if >20% of its sensor fetches fail.
  Auth errors (401/403) still fail the run immediately.
- G1/G4 — the load appends verbatim page bodies into a JSON column through a
  temp external table; `_FILE_NAME` becomes `source_uri`. That column is
  load-bearing: measurement payloads carry no sensor/location ids, identity
  rides on the GCS object path. Same-day reruns append a new
  `ingested_at` batch; dedup is dbt staging's job.
- G9 — the load emits an Airflow Dataset; Phase 4's transform DAG schedules
  on it (data-aware scheduling, no TriggerDagRunOperator).

Each run ingests its logical date `ds` = the previous, completed UTC day
(the 02:00 UTC schedule gives 2h of grace for late arrivals). G4's rolling
lookback (Phase 5): each sensor fetch actually covers the last LOOKBACK_DAYS
days ending at ds — same request count (a week of hourly records fits one
page), but late back-corrections are re-fetched and win staging's
latest-ingested_at dedup. The landed object stays keyed to ds, so one
calendar day's readings live in up to LOOKBACK_DAYS objects across ds
prefixes — expected, not a bug; staging dedups.
"""

import datetime as dt
import logging
import os

import pendulum
from openaq_datasets import RAW_MEASUREMENTS_DATASET

from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from ingestion.openaq.bq_load import (
    ensure_raw_table_sql,
    external_table_definition,
    load_sql,
    measurement_count_sql,
    raw_table_name,
)
from ingestion.openaq.client import OpenAQClient
from ingestion.openaq.config import load_settings
from ingestion.openaq.gcs import LOCATIONS_LEAF, RAW_PREFIX, make_raw_zone_writer
from ingestion.openaq.ingest import (
    extract_target_sensors,
    fetch_sensor_day,
    resolve_country_id,
)

logger = logging.getLogger(__name__)

COUNTRIES = ("AE", "PK")
GCP_CONN_ID = "google_cloud_default"  # defined via env in docker-compose (G10)
API_POOL = "openaq_api"  # 4 slots, created by airflow-init: 60 req/min budget
FETCH_MAX_ATTEMPTS = 2  # persistent-5xx sensors exist; deep retries are pure cost
FAILURE_RATE_THRESHOLD = 0.20
LOOKBACK_DAYS = 7  # G4 rolling lookback: re-fetch the last week in every run

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "")
BIGQUERY_RAW_DATASET = os.environ.get("BIGQUERY_RAW_DATASET", "openaq_raw")
# BQ jobs run in one location, which must equal the dataset's region exactly
# (same lesson as the dbt profiles.yml fix in the pre-Phase-2 audit).
BIGQUERY_LOCATION = os.environ.get("BIGQUERY_LOCATION", "us-central1")
# The load contract (temp external table, PARSE_JSON, _FILE_NAME→source_uri)
# lives in ingestion/openaq/bq_load.py, shared with the Phase 5 backfill CLI
# so the two load paths cannot drift.
RAW_TABLE = raw_table_name(GCP_PROJECT_ID, BIGQUERY_RAW_DATASET)


def _run_date() -> dt.date:
    """The UTC day this run ingests — the run's logical date (ds)."""
    return dt.date.fromisoformat(get_current_context()["ds"])


def _client_and_writer() -> tuple[OpenAQClient, object]:
    settings = load_settings()
    client = OpenAQClient(
        settings.api_key, settings.base_url, max_attempts=FETCH_MAX_ATTEMPTS
    )
    return client, make_raw_zone_writer(settings.bucket_name)


@task(retries=1)
def prepare_country_run(country_code: str) -> list[dict]:
    """Resolve the country, land the verbatim locations inventory, and emit
    the sensor list the mapped fetch tasks expand over.

    Returns only the plain list: dynamic mapping can expand solely over a
    task's default return_value XCom, not over keyed (multiple_outputs) ones.
    """
    client, writer = _client_and_writer()
    date = _run_date()
    country_id = resolve_country_id(client, country_code)
    location_responses = list(client.paginate("/locations", {"countries_id": country_id}))
    writer.write_pages(
        country_code, date, LOCATIONS_LEAF, [r.text for r in location_responses]
    )
    location_pages = [r.json() for r in location_responses]
    sensors = extract_target_sensors(location_pages)
    if not sensors:
        # An empty .expand() list is marked skipped, not failed, and the skip
        # cascades downstream — a zero-sensor country must be loud instead.
        raise RuntimeError(
            f"{country_code}: no target sensors found in the locations "
            "inventory — anomalous for an onboarded country, investigate"
        )
    n_locations = sum(len(p.get("results", [])) for p in location_pages)
    logger.info(
        "%s (countries_id=%s): %d locations, %d target sensors",
        country_code, country_id, n_locations, len(sensors),
    )
    return [{"sensor_id": s, "parameter": p} for s, p in sensors]


@task(pool=API_POOL, retries=1, execution_timeout=dt.timedelta(minutes=15))
def fetch_sensor(country_code: str, spec: dict) -> dict:
    """Fetch + land one sensor's lookback window ending at ds. API failures
    come back as data (status="failed"), not exceptions — see the module
    docstring."""
    client, writer = _client_and_writer()
    return fetch_sensor_day(
        client,
        writer,
        country_code,
        _run_date(),
        spec["sensor_id"],
        spec["parameter"],
        lookback_days=LOOKBACK_DAYS,
    )


@task
def summarize_country(country_code: str, results: list[dict]) -> dict:
    """Aggregate mapped-fetch outcomes; fail the run only past the threshold."""
    by_status: dict[str, list[dict]] = {"ok": [], "empty": [], "failed": []}
    for result in results:
        by_status[result["status"]].append(result)
    measurements = sum(r["measurements"] for r in by_status["ok"])
    failed_ids = sorted(r["sensor_id"] for r in by_status["failed"])
    total = len(results)
    logger.info(
        "%s: %d/%d sensors with data (%d empty), %d measurements",
        country_code, len(by_status["ok"]), total,
        len(by_status["empty"]), measurements,
    )
    if failed_ids:
        logger.error(
            "%s: %d sensor fetch(es) failed after retries: %s",
            country_code, len(failed_ids), failed_ids,
        )
    failure_rate = len(failed_ids) / total if total else 0.0
    if failure_rate > FAILURE_RATE_THRESHOLD:
        raise RuntimeError(
            f"{country_code}: {failure_rate:.0%} of sensor fetches failed "
            f"(threshold {FAILURE_RATE_THRESHOLD:.0%}) — abnormal, investigate"
        )
    return {
        "country_code": country_code,
        "measurements": measurements,
        "sensors_ok": len(by_status["ok"]),
        "sensors_empty": len(by_status["empty"]),
        "sensors_failed": len(failed_ids),
    }


@task
def reconcile_counts(summaries: list[dict]) -> None:
    """Exit-criterion check: measurement counts seen in API responses must
    equal what this run's batch landed in BigQuery."""
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    ds = get_current_context()["ds"]
    api_total = sum(s["measurements"] for s in summaries)
    # Batch isolation across same-day reruns (MAX(ingested_at)) lives in the
    # shared measurement_count_sql builder.
    sql = measurement_count_sql(
        RAW_TABLE, f"gs://{GCS_BUCKET_NAME}/{RAW_PREFIX}/%/{ds}/%"
    )
    # The native client, not hook.get_records(): the hook's DB-API layer
    # mishandles job location (job created in one location, polled in
    # another → 404, seen live). query_and_wait pins the location end-to-end.
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    client = hook.get_client(project_id=hook.project_id, location=BIGQUERY_LOCATION)
    rows = list(client.query_and_wait(sql))
    bq_total = int(rows[0][0])
    if bq_total != api_total:
        raise RuntimeError(
            f"count mismatch for {ds}: API run fetched {api_total} measurements, "
            f"BigQuery landed {bq_total}"
        )
    logger.info("reconciled %s: %d measurements in both the API run and BigQuery", ds, api_total)


@dag(
    dag_id="openaq_ingest",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openaq", "ingestion", "raw"],
    doc_md=__doc__,
)
def openaq_ingest():
    summaries = []
    for country in COUNTRIES:

        @task_group(group_id=f"ingest_{country.lower()}")
        def ingest_country(country_code: str):
            sensor_specs = prepare_country_run(country_code)
            results = fetch_sensor.partial(country_code=country_code).expand(
                spec=sensor_specs
            )
            return summarize_country(country_code, results)

        summaries.append(ingest_country(country))

    ensure_raw_table = BigQueryInsertJobOperator(
        task_id="ensure_raw_table",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {"query": ensure_raw_table_sql(RAW_TABLE), "useLegacySql": False}
        },
    )

    load_raw_to_bq = BigQueryInsertJobOperator(
        task_id="load_raw_to_bq",
        gcp_conn_id=GCP_CONN_ID,
        outlets=[RAW_MEASUREMENTS_DATASET],
        configuration={
            "query": {
                "query": load_sql(RAW_TABLE),
                "useLegacySql": False,
                "tableDefinitions": {
                    # Wildcard deliberately ends at /* (not /*.json) — the
                    # template_ext footgun; see external_table_definition.
                    "raw_lines": external_table_definition([
                        f"gs://{GCS_BUCKET_NAME}/{RAW_PREFIX}/{c}/{{{{ ds }}}}/*"
                        for c in COUNTRIES
                    ]),
                },
            },
        },
    )

    summaries >> ensure_raw_table >> load_raw_to_bq >> reconcile_counts(summaries)


openaq_ingest()
