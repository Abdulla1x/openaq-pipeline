"""Phase 5 backfill: wide-window history load, chunked and resumable.

Why this exists as a CLI and not a DAG (PROJECT_CONTEXT §6/§8): day-by-day
backfill costs 2+n_sensors requests per country-day — PK × 1yr ≈ 165k
requests ≈ 45h at the 60 req/min limit. Wide per-sensor windows collapse that
to one paginated request per sensor per chunk (an hourly sensor fits ~41 days
per 1000-record page) ≈ 1.5h/yr. The run is strictly rate-limited and
sequential, so Airflow's parallelism buys nothing and its per-task timeouts
would fight the wide fetches; missed-day catch-up was explicitly declared
backfill's job when the DAG chose catchup=False.

Unit of work: one (country, chunk) where a chunk is a half-open [start, end)
window (edges verified half-open against the live API 2026-07-18 — abutting
chunks never double-land a record). A chunk is checkpointed into a local JSON
state file only after fetch → land → load → reconcile all succeed, so an
interrupted run resumes at the first incomplete chunk; re-running a partially
fetched chunk is benign (append-only raw + staging dedup, G4).

Landing layout: raw/openaq/{country}/backfill/{start}_{end}/{sensor_id}.json
plus one locations.json per invocation under its first processed chunk.
Staging parses only country + leaf from source_uri, and the daily reconcile's
/{ds}/ filter cannot match the backfill/ segment — the namespaces coexist.
"""

import csv
import datetime as dt
import json
import logging
import os
from pathlib import Path

from ingestion.openaq.bq_load import (
    ensure_raw_table_sql,
    external_table_definition,
    load_sql,
    measurement_count_sql,
    raw_table_name,
)
from ingestion.openaq.gcs import LOCATIONS_LEAF, RAW_PREFIX
from ingestion.openaq.ingest import (
    extract_target_sensors,
    fetch_sensor_window,
    resolve_country_id,
)

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_DAYS = 60
# Same catch+threshold rationale as the DAG: known-bad sensors are normal,
# a majority failing is not.
FAILURE_RATE_THRESHOLD = 0.20


def plan_chunks(
    start: dt.date, end: dt.date, chunk_days: int = DEFAULT_CHUNK_DAYS
) -> list[tuple[dt.date, dt.date]]:
    """Abutting half-open [start, end) chunks; the last one is truncated."""
    if start >= end:
        raise ValueError(f"backfill window is empty: start={start} end={end}")
    if chunk_days < 1:
        raise ValueError(f"chunk_days must be positive, got {chunk_days}")
    chunks = []
    cursor = start
    while cursor < end:
        upper = min(cursor + dt.timedelta(days=chunk_days), end)
        chunks.append((cursor, upper))
        cursor = upper
    return chunks


def chunk_partition(window_start: dt.date, window_end: dt.date) -> str:
    return f"backfill/{window_start.isoformat()}_{window_end.isoformat()}"


def load_skip_sensors(csv_path: str) -> frozenset[int]:
    """Sensor ids from a CSV with a sensor_id column (the known_bad seed)."""
    with open(csv_path, newline="") as handle:
        return frozenset(int(row["sensor_id"]) for row in csv.DictReader(handle))


class BackfillState:
    """Chunk-level checkpoint file. A chunk appears only once fully verified."""

    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._records: list[dict] = []
        if self._path.exists():
            try:
                self._records = json.loads(self._path.read_text())["completed"]
            except (json.JSONDecodeError, KeyError, TypeError) as exc:
                raise RuntimeError(
                    f"backfill state file {self._path} is unreadable ({exc!r}); "
                    "delete or repair it to resume (completed chunks can be "
                    "re-verified against BigQuery counts)"
                ) from exc

    @staticmethod
    def _key(record: dict) -> tuple[str, str, str]:
        return (record["country"], record["window_start"], record["window_end"])

    def is_done(self, country: str, start: dt.date, end: dt.date) -> bool:
        key = (country, start.isoformat(), end.isoformat())
        return any(self._key(r) == key for r in self._records)

    def mark_done(self, record: dict) -> None:
        self._records.append(record)
        # Temp file + atomic rename: a crash mid-write must never corrupt the
        # checkpoint history that resume depends on.
        tmp_path = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp_path.write_text(json.dumps({"completed": self._records}, indent=2) + "\n")
        os.replace(tmp_path, self._path)


class RawLoader:
    """The CLI's BigQuery side of the shared load contract (bq_load.py).

    Kept as one small object so tests can fake the BQ surface; the real
    implementation pins every job to the dataset's location (BQ jobs run in
    exactly one location — the twice-learned Phase 2/3 lesson).
    """

    def __init__(self, project_id: str, dataset: str, location: str):
        from google.cloud import bigquery

        self._bigquery = bigquery
        self._client = bigquery.Client(project=project_id, location=location)
        self.raw_table = raw_table_name(project_id, dataset)

    def ensure_table(self) -> None:
        self._client.query_and_wait(ensure_raw_table_sql(self.raw_table))

    def load_prefix(self, source_uris: list[str]) -> None:
        config = self._bigquery.QueryJobConfig(
            table_definitions={
                "raw_lines": self._bigquery.ExternalConfig.from_api_repr(
                    external_table_definition(source_uris)
                )
            }
        )
        self._client.query_and_wait(load_sql(self.raw_table), job_config=config)

    def count_measurements(self, uri_like: str) -> int:
        rows = list(
            self._client.query_and_wait(measurement_count_sql(self.raw_table, uri_like))
        )
        return int(rows[0][0])


def make_raw_loader() -> RawLoader:
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        raise RuntimeError("GCP_PROJECT_ID is required for the backfill BQ load")
    return RawLoader(
        project_id=project_id,
        dataset=os.environ.get("BIGQUERY_RAW_DATASET", "openaq_raw"),
        location=os.environ.get("BIGQUERY_LOCATION", "us-central1"),
    )


def run_backfill(
    client,
    writer,
    loader,
    state: BackfillState,
    country_code: str,
    start: dt.date,
    end: dt.date,
    *,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    skip_sensors: frozenset[int] = frozenset(),
    bucket_name: str,
) -> int:
    """Backfill one country over [start, end). Returns a process exit code."""
    chunks = plan_chunks(start, end, chunk_days)
    todo = [c for c in chunks if not state.is_done(country_code, *c)]
    logger.info(
        "%s backfill [%s, %s): %d chunk(s) of ≤%dd, %d already complete",
        country_code, start, end, len(chunks), chunk_days, len(chunks) - len(todo),
    )
    if not todo:
        logger.info("%s: nothing to do — all chunks checkpointed", country_code)
        return 0

    country_id = resolve_country_id(client, country_code)
    location_responses = list(client.paginate("/locations", {"countries_id": country_id}))
    sensors = extract_target_sensors([r.json() for r in location_responses])
    if not sensors:
        raise RuntimeError(f"{country_code}: no target sensors in the locations inventory")
    # One inventory landing per invocation, under the first chunk it processes;
    # dbt needs at least one locations.json per load path for the sensor→location join.
    inventory_partition = chunk_partition(*todo[0])
    writer.write_pages(
        country_code, inventory_partition, LOCATIONS_LEAF,
        [r.text for r in location_responses],
    )
    if skip_sensors:
        logger.info(
            "%s: skipping %d known-bad sensor(s) present in the skip list",
            country_code, len(set(s for s, _ in sensors) & skip_sensors),
        )
    loader.ensure_table()

    for window_start, window_end in todo:
        partition = chunk_partition(window_start, window_end)
        api_count = objects = empty = 0
        failed: list[int] = []
        fetched = []
        for sensor_id, parameter in sensors:
            if sensor_id in skip_sensors:
                continue
            fetched.append(sensor_id)
            result = fetch_sensor_window(
                client, writer, country_code, partition,
                window_start, window_end, sensor_id, parameter,
            )
            if result["status"] == "failed":
                failed.append(sensor_id)
            elif result["status"] == "ok":
                api_count += result["measurements"]
                objects += 1
            else:
                empty += 1
        failure_rate = len(failed) / len(fetched) if fetched else 0.0
        if failure_rate > FAILURE_RATE_THRESHOLD:
            logger.error(
                "%s %s: %.0f%% of sensor fetches failed (threshold %.0f%%) — "
                "chunk NOT checkpointed: %s",
                country_code, partition, failure_rate * 100,
                FAILURE_RATE_THRESHOLD * 100, sorted(failed),
            )
            return 1

        prefix = f"{RAW_PREFIX}/{country_code}/{partition}"
        has_inventory = partition == inventory_partition
        if objects or has_inventory:
            loader.load_prefix([f"gs://{bucket_name}/{prefix}/*"])
        bq_count = loader.count_measurements(f"gs://{bucket_name}/{prefix}/%")
        if bq_count != api_count:
            logger.error(
                "%s %s: count mismatch — API fetched %d, BigQuery landed %d; "
                "chunk NOT checkpointed",
                country_code, partition, api_count, bq_count,
            )
            return 1

        state.mark_done({
            "country": country_code,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "measurements": api_count,
            "sensors_with_data": objects,
            "sensors_empty": empty,
            "sensors_failed": sorted(failed),
            "completed_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        })
        logger.info(
            "%s %s: checkpointed — %d measurements, %d/%d sensors with data "
            "(%d empty, %d failed)",
            country_code, partition, api_count, objects, len(fetched), empty, len(failed),
        )
    return 0
