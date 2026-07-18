# ingestion/

Python ingestion layer for the OpenAQ v3 API (Phase 2 — implemented).

## Contents

```
ingestion/
├── constants.py    WHO 2021 air-quality guideline thresholds — mirror of the
│                   dbt seed (source of truth since Phase 4), sync-enforced by
│                   tests/unit/test_who_seed_sync.py
└── openaq/
    ├── config.py    settings from the environment (.env locally, docker-compose
    │                in the Airflow containers)
    ├── client.py    HTTP client: auth, header-driven rate throttling (60 req/min),
    │                429/5xx bounded retries, 401/403 fail-fast, pagination
    ├── ingest.py    G2 fan-out: countries_id → locations → target sensors
    │                (pm25/pm10/no2) → per-sensor half-open window measurements
    │                (the daily DAG fetches a 7-day lookback window — G4)
    ├── gcs.py       raw-zone writer: object naming contract + NDJSON upload
    ├── bq_load.py   the GCS→BQ load contract (external table + PARSE_JSON),
    │                shared by the DAG and the backfill CLI so they can't drift
    ├── backfill.py  Phase 5: chunked, resumable, reconciled history load
    └── __main__.py  CLI entry point (ingest / backfill subcommands)
```

One **parameterized** fetcher for any country (guardrail G12 — no per-country
modules). OpenAQ v3 is sensor-centric (G2): there is no flat measurements
endpoint; the client resolves the numeric `countries_id`, pages
`/v3/locations` (whose responses embed each location's sensor list), and
fetches `/v3/sensors/{id}/measurements` per target sensor.

## Raw zone contract (consumed by Phase 3)

```
gs://{bucket}/raw/openaq/{country}/{YYYY-MM-DD}/{sensor_id}.json   daily measurements
gs://{bucket}/raw/openaq/{country}/{YYYY-MM-DD}/locations.json     sensor→location inventory
gs://{bucket}/raw/openaq/{country}/backfill/{start}_{end}/…        backfill chunks (Phase 5)
```

dbt staging parses only the country and the leaf from `source_uri`
(measurement timestamps come from the payload), so the day and backfill
window segments are organizational — load globs and reconcile filters —
and the two shapes coexist safely.

Object content is newline-delimited **verbatim API page bodies** (G1 —
schema-on-read; no parsing at ingest time). Measurement payloads carry no
sensor/location ids, so identity rides on the object path and the landed
locations pages provide the sensor→location mapping for dbt. Sensors with no
measurements in the window write nothing (no empty objects in the raw zone).

## Running manually

```bash
set -a && source .env && set +a       # OPENAQ_API_KEY, GCS_BUCKET_NAME, GOOGLE_APPLICATION_CREDENTIALS
python -m ingestion.openaq ingest --country AE --date 2026-07-12   # --date defaults to yesterday (UTC)

# Chunked, resumable history load (Phase 5). Interrupt freely: completed
# chunks are checkpointed in backfill_state.json (gitignored) and skipped on
# re-run; each chunk is loaded to BigQuery and count-reconciled before being
# marked done. --skip-sensors-csv avoids burning retries on the known-bad
# sensor list (dbt/seeds/known_bad_sensors.csv).
python -m ingestion.openaq backfill --country PK --start 2025-06-01 \
    --skip-sensors-csv dbt/seeds/known_bad_sensors.csv
```

Backfill math (PROJECT_CONTEXT §8): a country-day costs `2 + n_sensors`
requests, so day-by-day history is unaffordable at 60 req/min (PK×1yr ≈ 45h);
wide per-sensor windows make it ~1 request per sensor per 60-day chunk
(PK×1yr ≈ 1.5h). Window edges are half-open — `datetime_to` is exclusive,
verified live 2026-07-18 — so abutting chunks never double-land a record.

Unit tests (API fully mocked, no network or credentials): `pytest tests/unit/`.
