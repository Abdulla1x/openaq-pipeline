"""One live round-trip: API contract → GCS raw landing → the BQ load contract.

The Phase 5 exit-criterion integration test. It exercises the field paths and
load mechanics whose silent drift is the realistic production breakage:

1. /v3/locations for AE — the contract fields staging + sensor discovery
   depend on (results[].id, sensors[].id, sensors[].parameter.name/units).
2. One real sensor's last-2-days window — the measurement contract fields
   (value, period.datetimeFrom.utc, period.label, coverage.observedCount).
3. Lands the fetched pages verbatim under raw/openaq/AE/_integration/{uuid}/
   — a prefix no production load glob (/{ds}/ or /backfill/) can ever match.
4. Queries them through an inline external table built from the exact
   production load config (bq_load.external_table_definition + PARSE_JSON) —
   the real load path mechanics without writing to raw_measurements.
5. Deletes the objects.

Cost: ~4-6 API requests + one BQ query. Needs a sourced .env; skips itself
(rather than erroring) when credentials are absent. Deliberately not in CI —
CI has no credentials by design (tests/README.md).
"""

import datetime as dt
import os
import uuid

import pytest

from ingestion.openaq.bq_load import external_table_definition
from ingestion.openaq.client import OpenAQClient
from ingestion.openaq.config import load_settings
from ingestion.openaq.gcs import RAW_PREFIX, RawZoneWriter
from ingestion.openaq.ingest import extract_target_sensors

pytestmark = pytest.mark.integration

AE_COUNTRIES_ID = 59  # verified live 2026-07-13 (PROJECT_CONTEXT §9)

_REQUIRED = ("OPENAQ_API_KEY", "OPENAQ_API_BASE_URL", "GCS_BUCKET_NAME", "GCP_PROJECT_ID")


def _credentials_present() -> bool:
    if not all(os.environ.get(v) for v in _REQUIRED):
        return False
    # CLI runs carry the host key path in GCP_KEY_FILE; ADC may also be set.
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and os.environ.get("GCP_KEY_FILE"):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ["GCP_KEY_FILE"]
    return True


@pytest.mark.skipif(not _credentials_present(), reason="needs a sourced .env with live credentials")
def test_live_roundtrip():
    from google.cloud import bigquery, storage

    settings = load_settings()
    client = OpenAQClient(api_key=settings.api_key, base_url=settings.base_url, max_attempts=2)

    # --- 1. locations contract -------------------------------------------
    location_responses = list(
        client.paginate("/locations", {"countries_id": AE_COUNTRIES_ID})
    )
    pages = [r.json() for r in location_responses]
    results = [loc for page in pages for loc in page.get("results", [])]
    assert results, "AE locations inventory came back empty"
    assert all("id" in loc for loc in results)
    sensors = extract_target_sensors(pages)
    assert sensors, "no pm25/pm10/no2 sensors in the AE inventory"
    a_sensor = next(  # a sensor whose parameter metadata carries units
        s for page in pages for loc in page.get("results", [])
        for s in loc.get("sensors", []) if s.get("parameter")
    )
    assert "units" in a_sensor["parameter"], "sensor parameter lost its units field"

    # --- 2. measurements contract ----------------------------------------
    today = dt.datetime.now(dt.UTC).date()
    window = {
        "datetime_from": f"{(today - dt.timedelta(days=2)).isoformat()}T00:00:00Z",
        "datetime_to": f"{today.isoformat()}T00:00:00Z",
    }
    fetched_records = 0
    measurement_bodies: list[str] = []
    # Stop at the first sensor with data. Most AE sensors are dormant (a
    # Phase 2 finding), so scan the whole list — empties cost one fast
    # request each and the loop breaks on the first hit.
    for sensor_id, _ in sensors:
        responses_for_sensor = list(
            client.paginate(f"/sensors/{sensor_id}/measurements", window)
        )
        records = [
            rec for resp in responses_for_sensor
            for rec in resp.json().get("results", [])
        ]
        if records:
            record = records[0]
            assert "value" in record
            assert record["period"]["datetimeFrom"]["utc"]
            assert "label" in record["period"]
            assert "observedCount" in record["coverage"]
            fetched_records = len(records)
            measurement_bodies = [
                r.text for r in responses_for_sensor if r.json().get("results")
            ]
            landed_sensor_id = sensor_id
            break
    assert fetched_records, "no AE sensor had data in the last 2 days — rerun later"

    # --- 3 + 4. land verbatim, read back through the load contract --------
    bucket = storage.Client().bucket(settings.bucket_name)
    writer = RawZoneWriter(bucket)
    partition = f"_integration/{uuid.uuid4().hex[:12]}"
    prefix = f"{RAW_PREFIX}/AE/{partition}"
    uris = [
        writer.write_pages("AE", partition, landed_sensor_id, measurement_bodies),
        writer.write_pages("AE", partition, "locations", [r.text for r in location_responses]),
    ]
    try:
        bq = bigquery.Client(
            project=os.environ["GCP_PROJECT_ID"],
            location=os.environ.get("BIGQUERY_LOCATION", "us-central1"),
        )
        config = bigquery.QueryJobConfig(
            table_definitions={
                "raw_lines": bigquery.ExternalConfig.from_api_repr(
                    external_table_definition(
                        [f"gs://{settings.bucket_name}/{prefix}/*"]
                    )
                )
            }
        )
        rows = list(bq.query_and_wait(
            """
            SELECT
              COUNT(*) AS page_rows,
              COALESCE(SUM(ARRAY_LENGTH(JSON_QUERY_ARRAY(
                PARSE_JSON(line, wide_number_mode => 'round'), '$.results'))), 0) AS records
            FROM raw_lines
            -- the same identity mechanism production uses (_FILE_NAME → source_uri)
            WHERE _FILE_NAME NOT LIKE '%/locations.json'
            """,
            job_config=config,
        ))
        total_pages = len(measurement_bodies) + len(location_responses)
        all_rows = list(bq.query_and_wait(
            "SELECT COUNT(*) FROM raw_lines",
            job_config=config,
        ))
        assert int(all_rows[0][0]) == total_pages, "a landed NDJSON line went missing in the load"
        assert int(rows[0][1]) == fetched_records, "measurement records lost between API and BQ"
    finally:
        # --- 5. teardown ---------------------------------------------------
        for blob in bucket.list_blobs(prefix=prefix):
            blob.delete()
    assert not list(bucket.list_blobs(prefix=prefix))
    assert all(u.startswith("gs://") for u in uris)
