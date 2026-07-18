"""The G2 fan-out against a fully mocked API: country resolution, sensor
filtering, empty-response safety (Phase 2 exit criterion), verbatim landing."""

import datetime as dt

import pytest
import responses

from ingestion.openaq.client import OpenAQAuthError, OpenAQClient
from ingestion.openaq.ingest import (
    extract_target_sensors,
    fetch_sensor_day,
    fetch_sensor_window,
    ingest_country_day,
    resolve_country_id,
)

BASE = "https://api.test/v3"
DATE = dt.date(2026, 7, 12)

COUNTRIES = {"results": [{"id": 59, "code": "AE"}, {"id": 109, "code": "PK"}]}
# Sensor 11 = pm25 (has data), 13 = no2 (empty), 12 = temperature (filtered out)
LOCATIONS_BODY = (
    '{"results": ['
    '{"id": 1, "name": "L1", "sensors": ['
    '{"id": 11, "parameter": {"name": "pm25"}}, {"id": 12, "parameter": {"name": "temperature"}}'
    ']},'
    '{"id": 2, "name": "L2", "sensors": [{"id": 13, "parameter": {"name": "no2"}}]}'
    "]}"
)
SENSOR_11_BODY = '{"results": [{"value": 49.1},  {"value": 50.0}]}'  # odd spacing on purpose


class FakeWriter:
    def __init__(self):
        self.writes = []

    def write_pages(self, country_code, date, leaf, page_bodies):
        self.writes.append((country_code, date, leaf, page_bodies))
        return f"gs://fake/raw/openaq/{country_code}/{date}/{leaf}.json"


def make_client() -> OpenAQClient:
    return OpenAQClient(api_key="k", base_url=BASE, sleep=lambda _: None)


@responses.activate
def test_resolve_country_id():
    responses.get(f"{BASE}/countries", json=COUNTRIES)
    assert resolve_country_id(make_client(), "PK") == 109


@responses.activate
def test_resolve_unknown_country_raises():
    responses.get(f"{BASE}/countries", json=COUNTRIES)
    with pytest.raises(ValueError, match="'XX' not found"):
        resolve_country_id(make_client(), "XX")


def test_extract_target_sensors_filters_and_sorts():
    pages = [
        {"results": [{"sensors": [
            {"id": 20, "parameter": {"name": "no2"}},
            {"id": 5, "parameter": {"name": "temperature"}},
        ]}]},
        {"results": [{"sensors": [
            {"id": 7, "parameter": {"name": "pm10"}},
            {"id": 20, "parameter": {"name": "no2"}},  # duplicate across pages
            {"id": 9, "parameter": None},  # malformed sensor entry must not crash
        ]}]},
    ]
    assert extract_target_sensors(pages) == [(7, "pm10"), (20, "no2")]


@responses.activate
def test_ingest_country_day_end_to_end():
    responses.get(f"{BASE}/countries", json=COUNTRIES)
    responses.get(f"{BASE}/locations", body=LOCATIONS_BODY, content_type="application/json")
    responses.get(f"{BASE}/sensors/11/measurements", body=SENSOR_11_BODY,
                  content_type="application/json")
    responses.get(f"{BASE}/sensors/13/measurements", json={"results": []})  # empty: must not crash

    writer = FakeWriter()
    summary = ingest_country_day(make_client(), writer, "AE", DATE)

    assert summary.locations == 2
    assert summary.sensors_targeted == 2
    assert summary.sensors_with_data == 1
    assert summary.sensors_empty == 1
    assert summary.measurements == 2
    assert len(summary.objects_written) == 2  # locations inventory + sensor 11

    leaves = [w[2] for w in writer.writes]
    assert leaves == ["locations", 11]  # nothing written for empty sensor 13
    assert writer.writes[0][3] == [LOCATIONS_BODY]  # verbatim, byte-for-byte
    assert writer.writes[1][3] == [SENSOR_11_BODY]

    measurements_url = next(
        c.request.url for c in responses.calls if "/sensors/11/" in c.request.url
    )
    assert "datetime_from=2026-07-12T00%3A00%3A00Z" in measurements_url
    assert "datetime_to=2026-07-13T00%3A00%3A00Z" in measurements_url


@responses.activate
def test_broken_sensor_is_isolated_not_fatal():
    """A sensor that persistently 500s (seen live: PK 15904590) must be
    recorded as failed while the remaining sensors are still fetched."""
    responses.get(f"{BASE}/countries", json=COUNTRIES)
    responses.get(f"{BASE}/locations", body=LOCATIONS_BODY, content_type="application/json")
    responses.get(f"{BASE}/sensors/11/measurements", status=500)  # every attempt
    responses.get(f"{BASE}/sensors/13/measurements", json={"results": [{"value": 1.0}]})

    writer = FakeWriter()
    summary = ingest_country_day(make_client(), writer, "AE", DATE)

    assert summary.sensors_failed == [11]
    assert summary.sensors_with_data == 1  # sensor 13 still landed
    assert [w[2] for w in writer.writes] == ["locations", 13]


# fetch_sensor_day is the unit of work Phase 3's mapped Airflow tasks call
# directly, so its dict contract is pinned here, not just via the CLI loop.


@responses.activate
def test_fetch_sensor_day_ok():
    responses.get(f"{BASE}/sensors/11/measurements", body=SENSOR_11_BODY,
                  content_type="application/json")
    writer = FakeWriter()
    result = fetch_sensor_day(make_client(), writer, "AE", DATE, 11, "pm25")
    assert result == {
        "sensor_id": 11,
        "parameter": "pm25",
        "status": "ok",
        "measurements": 2,
        "uri": "gs://fake/raw/openaq/AE/2026-07-12/11.json",
        "error": None,
    }
    assert writer.writes[0][3] == [SENSOR_11_BODY]  # verbatim page body


@responses.activate
def test_fetch_sensor_day_empty_writes_nothing():
    responses.get(f"{BASE}/sensors/13/measurements", json={"results": []})
    writer = FakeWriter()
    result = fetch_sensor_day(make_client(), writer, "AE", DATE, 13, "no2")
    assert result["status"] == "empty"
    assert result["uri"] is None
    assert writer.writes == []


@responses.activate
def test_fetch_sensor_day_failure_is_reported_not_raised():
    responses.get(f"{BASE}/sensors/11/measurements", status=500)  # every attempt
    result = fetch_sensor_day(make_client(), FakeWriter(), "AE", DATE, 11, "pm25")
    assert result["status"] == "failed"
    assert "attempts" in result["error"]
    assert result["measurements"] == 0


@responses.activate
def test_fetch_sensor_day_lookback_widens_window_keeps_ds_path():
    """G4 rolling lookback: the request window starts lookback_days−1 before
    the run date, but the landed object stays keyed to the run date."""
    responses.get(f"{BASE}/sensors/11/measurements", body=SENSOR_11_BODY,
                  content_type="application/json")
    writer = FakeWriter()
    result = fetch_sensor_day(
        make_client(), writer, "AE", DATE, 11, "pm25", lookback_days=7
    )
    url = responses.calls[0].request.url
    assert "datetime_from=2026-07-06T00%3A00%3A00Z" in url  # DATE − 6
    assert "datetime_to=2026-07-13T00%3A00%3A00Z" in url  # DATE + 1
    assert result["uri"] == "gs://fake/raw/openaq/AE/2026-07-12/11.json"
    assert writer.writes[0][1] == DATE  # partition unchanged by lookback


@responses.activate
def test_fetch_sensor_window_uses_half_open_edges_and_partition():
    """Backfill's unit of work: an arbitrary [start, end) window landed under
    an arbitrary path partition (window edges verified half-open, 2026-07-18)."""
    responses.get(f"{BASE}/sensors/11/measurements", body=SENSOR_11_BODY,
                  content_type="application/json")
    writer = FakeWriter()
    result = fetch_sensor_window(
        make_client(), writer, "PK",
        partition="backfill/2025-06-01_2025-07-31",
        window_start=dt.date(2025, 6, 1),
        window_end=dt.date(2025, 7, 31),
        sensor_id=11, parameter="pm25",
    )
    url = responses.calls[0].request.url
    assert "datetime_from=2025-06-01T00%3A00%3A00Z" in url
    assert "datetime_to=2025-07-31T00%3A00%3A00Z" in url
    assert result["status"] == "ok"
    assert writer.writes[0][1] == "backfill/2025-06-01_2025-07-31"


@responses.activate
def test_auth_error_mid_fanout_is_fatal():
    responses.get(f"{BASE}/countries", json=COUNTRIES)
    responses.get(f"{BASE}/locations", body=LOCATIONS_BODY, content_type="application/json")
    responses.get(f"{BASE}/sensors/11/measurements", status=401)

    with pytest.raises(OpenAQAuthError):
        ingest_country_day(make_client(), FakeWriter(), "AE", DATE)
