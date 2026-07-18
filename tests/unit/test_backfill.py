"""Backfill mechanics: chunk math, checkpoint/resume, threshold + reconcile
guards. The API is mocked (G12); GCS and BigQuery sit behind fake seams."""

import datetime as dt
import json

import pytest
import responses

from ingestion.openaq.backfill import (
    BackfillState,
    chunk_partition,
    load_skip_sensors,
    plan_chunks,
    run_backfill,
)
from ingestion.openaq.client import OpenAQClient

BASE = "https://api.test/v3"

COUNTRIES_BODY = {"results": [{"id": 59, "code": "AE"}, {"id": 109, "code": "PK"}]}
LOCATIONS_BODY = (
    '{"results": ['
    '{"id": 1, "sensors": [{"id": 11, "parameter": {"name": "pm25"}}]},'
    '{"id": 2, "sensors": [{"id": 13, "parameter": {"name": "no2"}}]}'
    "]}"
)
SENSOR_11_BODY = '{"results": [{"value": 49.1}, {"value": 50.0}]}'

START = dt.date(2026, 1, 1)
END = dt.date(2026, 1, 5)


class FakeWriter:
    def __init__(self):
        self.writes = []

    def write_pages(self, country_code, partition, leaf, page_bodies):
        self.writes.append((country_code, partition, leaf, page_bodies))
        return f"gs://fake/raw/openaq/{country_code}/{partition}/{leaf}.json"


class FakeLoader:
    """Records load calls; count_measurements pops pre-seeded results."""

    def __init__(self, counts):
        self.counts = list(counts)
        self.loaded_uris = []
        self.ensured = 0

    def ensure_table(self):
        self.ensured += 1

    def load_prefix(self, source_uris):
        self.loaded_uris.extend(source_uris)

    def count_measurements(self, uri_like):
        return self.counts.pop(0)


def make_client() -> OpenAQClient:
    return OpenAQClient(api_key="k", base_url=BASE, sleep=lambda _: None)


def mock_inventory():
    responses.get(f"{BASE}/countries", json=COUNTRIES_BODY)
    responses.get(f"{BASE}/locations", body=LOCATIONS_BODY, content_type="application/json")


def test_plan_chunks_abutting_half_open_truncated():
    chunks = plan_chunks(START, END, chunk_days=2)
    assert chunks == [
        (dt.date(2026, 1, 1), dt.date(2026, 1, 3)),
        (dt.date(2026, 1, 3), dt.date(2026, 1, 5)),
    ]
    # A non-multiple range truncates the last chunk instead of overshooting.
    assert plan_chunks(START, END, chunk_days=3) == [
        (dt.date(2026, 1, 1), dt.date(2026, 1, 4)),
        (dt.date(2026, 1, 4), dt.date(2026, 1, 5)),
    ]


def test_plan_chunks_rejects_empty_or_inverted_window():
    with pytest.raises(ValueError):
        plan_chunks(START, START)
    with pytest.raises(ValueError):
        plan_chunks(END, START)
    with pytest.raises(ValueError):
        plan_chunks(START, END, chunk_days=0)


def test_chunk_partition_format():
    assert chunk_partition(START, dt.date(2026, 3, 2)) == "backfill/2026-01-01_2026-03-02"


def test_state_roundtrip_and_resume(tmp_path):
    path = tmp_path / "state.json"
    state = BackfillState(path)
    assert not state.is_done("AE", START, END)
    state.mark_done({
        "country": "AE",
        "window_start": START.isoformat(),
        "window_end": END.isoformat(),
        "measurements": 2,
    })
    # A fresh instance reads the checkpoint back from disk.
    assert BackfillState(path).is_done("AE", START, END)
    assert not BackfillState(path).is_done("PK", START, END)
    assert json.loads(path.read_text())["completed"][0]["measurements"] == 2


def test_load_skip_sensors(tmp_path):
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text(
        "sensor_id,country_code,first_seen_failing,last_verified_failing,note\n"
        "15904590,PK,2026-07-13,2026-07-17,persistent 500\n"
        "16034748,PK,2026-07-15,2026-07-17,persistent 500\n"
    )
    assert load_skip_sensors(str(csv_path)) == frozenset({15904590, 16034748})


@responses.activate
def test_run_backfill_happy_path_checkpoints_each_chunk(tmp_path):
    mock_inventory()
    responses.get(f"{BASE}/sensors/11/measurements", body=SENSOR_11_BODY,
                  content_type="application/json")
    responses.get(f"{BASE}/sensors/13/measurements", json={"results": []})

    writer, loader = FakeWriter(), FakeLoader(counts=[2, 2])
    state = BackfillState(tmp_path / "state.json")
    code = run_backfill(
        make_client(), writer, loader, state, "AE", START, END,
        chunk_days=2, bucket_name="test-bucket",
    )

    assert code == 0
    assert loader.ensured == 1
    assert state.is_done("AE", dt.date(2026, 1, 1), dt.date(2026, 1, 3))
    assert state.is_done("AE", dt.date(2026, 1, 3), dt.date(2026, 1, 5))
    # locations inventory landed exactly once, under the first chunk.
    inventory = [w for w in writer.writes if w[2] == "locations"]
    assert [w[1] for w in inventory] == ["backfill/2026-01-01_2026-01-03"]
    # Both chunk prefixes were loaded with /* globs (template_ext contract).
    assert loader.loaded_uris == [
        "gs://test-bucket/raw/openaq/AE/backfill/2026-01-01_2026-01-03/*",
        "gs://test-bucket/raw/openaq/AE/backfill/2026-01-03_2026-01-05/*",
    ]


@responses.activate
def test_run_backfill_resumes_skipping_completed_chunks(tmp_path):
    mock_inventory()
    responses.get(f"{BASE}/sensors/11/measurements", body=SENSOR_11_BODY,
                  content_type="application/json")
    responses.get(f"{BASE}/sensors/13/measurements", json={"results": []})

    state = BackfillState(tmp_path / "state.json")
    state.mark_done({
        "country": "AE",
        "window_start": "2026-01-01",
        "window_end": "2026-01-03",
    })
    writer = FakeWriter()
    code = run_backfill(
        make_client(), writer, FakeLoader(counts=[2]), state, "AE", START, END,
        chunk_days=2, bucket_name="test-bucket",
    )

    assert code == 0
    sensor_urls = [c.request.url for c in responses.calls if "/sensors/" in c.request.url]
    assert all("2026-01-03" in url and "2026-01-05" in url for url in sensor_urls)
    # The inventory lands under the first *incomplete* chunk.
    inventory = [w for w in writer.writes if w[2] == "locations"]
    assert [w[1] for w in inventory] == ["backfill/2026-01-03_2026-01-05"]


@responses.activate
def test_run_backfill_all_done_is_a_noop(tmp_path):
    state = BackfillState(tmp_path / "state.json")
    for ws, we in plan_chunks(START, END, chunk_days=2):
        state.mark_done({
            "country": "AE",
            "window_start": ws.isoformat(),
            "window_end": we.isoformat(),
        })
    code = run_backfill(
        make_client(), FakeWriter(), FakeLoader(counts=[]), state, "AE", START, END,
        chunk_days=2, bucket_name="test-bucket",
    )
    assert code == 0
    assert not responses.calls  # no API traffic at all


@responses.activate
def test_run_backfill_failure_threshold_aborts_without_checkpoint(tmp_path):
    mock_inventory()
    responses.get(f"{BASE}/sensors/11/measurements", status=500)  # 1 of 2 = 50%
    responses.get(f"{BASE}/sensors/13/measurements", json={"results": []})

    state = BackfillState(tmp_path / "state.json")
    code = run_backfill(
        make_client(), FakeWriter(), FakeLoader(counts=[]), state, "AE", START, END,
        chunk_days=2, bucket_name="test-bucket",
    )
    assert code == 1
    assert not state.is_done("AE", dt.date(2026, 1, 1), dt.date(2026, 1, 3))


@responses.activate
def test_run_backfill_reconcile_mismatch_aborts_without_checkpoint(tmp_path):
    mock_inventory()
    responses.get(f"{BASE}/sensors/11/measurements", body=SENSOR_11_BODY,
                  content_type="application/json")
    responses.get(f"{BASE}/sensors/13/measurements", json={"results": []})

    state = BackfillState(tmp_path / "state.json")
    code = run_backfill(
        make_client(), FakeWriter(), FakeLoader(counts=[999]), state, "AE", START, END,
        chunk_days=2, bucket_name="test-bucket",
    )
    assert code == 1
    assert not state.is_done("AE", dt.date(2026, 1, 1), dt.date(2026, 1, 3))


@responses.activate
def test_run_backfill_skip_sensors_are_never_fetched(tmp_path):
    mock_inventory()
    responses.get(f"{BASE}/sensors/13/measurements", json={"results": []})

    state = BackfillState(tmp_path / "state.json")
    code = run_backfill(
        make_client(), FakeWriter(), FakeLoader(counts=[0, 0]), state, "AE", START, END,
        chunk_days=2, skip_sensors=frozenset({11}), bucket_name="test-bucket",
    )
    assert code == 0
    assert not any("/sensors/11/" in c.request.url for c in responses.calls)
