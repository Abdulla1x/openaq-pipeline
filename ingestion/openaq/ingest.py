"""The G2 fan-out for one country and one UTC day.

Flow (guardrail G2 — v3 is sensor-centric, no flat measurements endpoint):
/v3/countries → numeric countries_id → /v3/locations?countries_id=… (whose
responses already embed each location's sensors, so no per-location call) →
/v3/sensors/{id}/measurements over the day window.

One parameterized entry point for any country (G12 — no per-country modules).
Measurement payloads carry no sensor/location ids (verified 2026-07-13), so
identity rides on the GCS object path, and the verbatim locations pages are
landed alongside the measurements to give dbt the sensor→location mapping.
"""

import datetime as dt
import logging
from dataclasses import dataclass, field

import requests

from ingestion.openaq.client import OpenAQAuthError, OpenAQClient
from ingestion.openaq.gcs import LOCATIONS_LEAF, RawZoneWriter

logger = logging.getLogger(__name__)

# The pollutants the marts compare (see ingestion/constants.py for their WHO
# thresholds). Filtering happens on sensor *metadata* — which sensors to fetch —
# not on fetched data, so G7 (completeness is never a silent filter) holds.
TARGET_PARAMETERS = frozenset({"pm25", "pm10", "no2"})


@dataclass
class RunSummary:
    country_code: str
    date: dt.date
    locations: int = 0
    sensors_targeted: int = 0
    sensors_with_data: int = 0
    sensors_empty: int = 0
    measurements: int = 0
    objects_written: list[str] = field(default_factory=list)
    # Sensors whose fetch failed after all retries (e.g. a sensor that
    # persistently 500s server-side, observed live: PK sensor 15904590).
    # One broken sensor must not abort the other ~450 — but the run still
    # reports failure (CLI exits non-zero) so orchestration notices.
    sensors_failed: list[int] = field(default_factory=list)


def resolve_country_id(client: OpenAQClient, country_code: str) -> int:
    for response in client.paginate("/countries"):
        for country in response.json().get("results", []):
            if country.get("code") == country_code:
                return country["id"]
    raise ValueError(f"country code {country_code!r} not found in /v3/countries")


def extract_target_sensors(location_pages: list[dict]) -> list[tuple[int, str]]:
    """(sensor_id, parameter) for every target-pollutant sensor, sorted by id."""
    sensors = {}
    for page in location_pages:
        for location in page.get("results", []):
            for sensor in location.get("sensors", []):
                parameter = (sensor.get("parameter") or {}).get("name")
                if parameter in TARGET_PARAMETERS:
                    sensors[sensor["id"]] = parameter
    return sorted(sensors.items())


def fetch_sensor_day(
    client: OpenAQClient,
    writer: RawZoneWriter,
    country_code: str,
    date: dt.date,
    sensor_id: int,
    parameter: str,
) -> dict:
    """Fetch one sensor's measurements for one UTC day and land them raw.

    The unit of work for Phase 3's dynamically mapped Airflow tasks (G3), so
    the return value is a plain JSON-serializable dict (XCom-safe):
    ``status`` is ``"ok"`` (object written), ``"empty"`` (no data, nothing
    written), or ``"failed"`` (fetch failed after retries — isolated per G12's
    fault-isolation decision; auth errors still propagate, since bad
    credentials fail every sensor and isolating them is pointless).
    """
    result = {
        "sensor_id": sensor_id,
        "parameter": parameter,
        "status": "empty",
        "measurements": 0,
        "uri": None,
        "error": None,
    }
    window = {
        "datetime_from": f"{date.isoformat()}T00:00:00Z",
        "datetime_to": f"{(date + dt.timedelta(days=1)).isoformat()}T00:00:00Z",
    }
    page_bodies: list[str] = []
    try:
        for response in client.paginate(f"/sensors/{sensor_id}/measurements", window):
            page_records = len(response.json().get("results", []))
            if page_records:
                page_bodies.append(response.text)
                result["measurements"] += page_records
    except OpenAQAuthError:
        raise
    except (RuntimeError, requests.RequestException) as exc:
        result.update(status="failed", measurements=0, error=str(exc))
        logger.error("sensor %s (%s): fetch failed: %s", sensor_id, parameter, exc)
        return result
    if page_bodies:
        result["uri"] = writer.write_pages(country_code, date, sensor_id, page_bodies)
        result["status"] = "ok"
    else:
        logger.debug("sensor %s (%s): no measurements, skipped", sensor_id, parameter)
    return result


def ingest_country_day(
    client: OpenAQClient, writer: RawZoneWriter, country_code: str, date: dt.date
) -> RunSummary:
    summary = RunSummary(country_code=country_code, date=date)
    country_id = resolve_country_id(client, country_code)
    logger.info("%s resolved to countries_id=%s", country_code, country_id)

    location_responses = list(client.paginate("/locations", {"countries_id": country_id}))
    location_pages = [r.json() for r in location_responses]
    summary.locations = sum(len(p.get("results", [])) for p in location_pages)
    uri = writer.write_pages(
        country_code, date, LOCATIONS_LEAF, [r.text for r in location_responses]
    )
    summary.objects_written.append(uri)

    sensors = extract_target_sensors(location_pages)
    summary.sensors_targeted = len(sensors)
    logger.info(
        "%s: %d locations, %d target sensors (%s)",
        country_code, summary.locations, len(sensors), ", ".join(sorted(TARGET_PARAMETERS)),
    )

    for sensor_id, parameter in sensors:
        result = fetch_sensor_day(client, writer, country_code, date, sensor_id, parameter)
        if result["status"] == "failed":
            summary.sensors_failed.append(sensor_id)
        elif result["status"] == "empty":
            summary.sensors_empty += 1
        else:
            summary.objects_written.append(result["uri"])
            summary.sensors_with_data += 1
            summary.measurements += result["measurements"]

    logger.info(
        "%s %s: %d/%d sensors had data (%d empty), %d measurements, %d objects written",
        country_code, date, summary.sensors_with_data, summary.sensors_targeted,
        summary.sensors_empty, summary.measurements, len(summary.objects_written),
    )
    if summary.sensors_failed:
        logger.error(
            "%s %s: %d sensor(s) failed after retries: %s",
            country_code, date, len(summary.sensors_failed), summary.sensors_failed,
        )
    return summary
