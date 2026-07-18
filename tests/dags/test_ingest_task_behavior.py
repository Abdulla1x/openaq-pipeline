"""Behavioral tests for the ingest DAG's failure model (catch + threshold).

The DagBag tests assert structure; these exercise the decision logic itself
via the decorated tasks' underlying functions — the 20% failure threshold and
the zero-sensor guard are the two riskiest branches in the DAG and were
previously untested. Same environment contract as test_dag_integrity.py.
"""

import datetime as dt
from types import SimpleNamespace

import pytest

# Skip on airflow.models, not "airflow": the repo's airflow/ directory is a
# namespace package on pytest's sys.path, so bare "airflow" always imports.
pytest.importorskip("airflow.models", reason="requires apache-airflow (dag-validate job)")

import openaq_ingest  # noqa: E402  (sys.path set up by conftest.py)


def _result(status: str, sensor_id: int, measurements: int = 0) -> dict:
    """A fetch_sensor_day outcome dict (the mapped task's XCom contract)."""
    return {"status": status, "sensor_id": sensor_id, "measurements": measurements}


def test_summarize_country_tolerates_failures_at_the_threshold():
    # 2 failed of 10 = exactly 20%: the threshold is strict >, so the run
    # stays green (mirrors live PK runs, where ~8% of sensors always fail).
    results = [_result("ok", i, measurements=5) for i in range(6)]
    results += [_result("empty", 6), _result("empty", 7)]
    results += [_result("failed", 8), _result("failed", 9)]

    summary = openaq_ingest.summarize_country.function("PK", results)

    assert summary == {
        "country_code": "PK",
        "measurements": 30,
        "sensors_ok": 6,
        "sensors_empty": 2,
        "sensors_failed": 2,
    }


def test_summarize_country_fails_the_run_past_the_threshold():
    results = [_result("ok", i, measurements=5) for i in range(7)]
    results += [_result("failed", i) for i in range(7, 10)]  # 3/10 = 30%

    with pytest.raises(RuntimeError, match="30% of sensor fetches failed"):
        openaq_ingest.summarize_country.function("PK", results)


@pytest.fixture
def stubbed_country_run(monkeypatch):
    """Stub the network/GCS edges of prepare_country_run; the sensor list its
    inventory yields is controlled per-test via extract_target_sensors."""
    client = SimpleNamespace(paginate=lambda path, params: iter([]))
    writer = SimpleNamespace(write_pages=lambda *args: None)
    monkeypatch.setattr(openaq_ingest, "_client_and_writer", lambda: (client, writer))
    monkeypatch.setattr(openaq_ingest, "_run_date", lambda: dt.date(2026, 7, 14))
    monkeypatch.setattr(openaq_ingest, "resolve_country_id", lambda c, code: 59)
    return monkeypatch


def test_prepare_country_run_emits_the_mapped_task_spec_shape(stubbed_country_run):
    stubbed_country_run.setattr(
        openaq_ingest, "extract_target_sensors", lambda pages: [(101, "pm25")]
    )

    specs = openaq_ingest.prepare_country_run.function("AE")

    # The exact shape fetch_sensor.expand() consumes — a plain list of dicts
    # (dynamic mapping can only expand over the default return_value XCom).
    assert specs == [{"sensor_id": 101, "parameter": "pm25"}]


def test_prepare_country_run_fails_loudly_on_zero_sensors(stubbed_country_run):
    # An empty .expand() list would be marked skipped (not failed) and the
    # skip cascades toward the load — a zero-sensor country must raise.
    stubbed_country_run.setattr(openaq_ingest, "extract_target_sensors", lambda pages: [])

    with pytest.raises(RuntimeError, match="no target sensors"):
        openaq_ingest.prepare_country_run.function("AE")
