# ingestion/

Python ingestion layer for the OpenAQ v3 API (Phase 2 — implemented).

## Contents

```
ingestion/
├── constants.py    WHO 2021 air-quality guideline thresholds (source of truth
│                   until the dbt seed supersedes it in Phase 4)
└── openaq/
    ├── config.py    settings from the environment (.env locally, compose later)
    ├── client.py    HTTP client: auth, header-driven rate throttling (60 req/min),
    │                429/5xx bounded retries, 401/403 fail-fast, pagination
    ├── ingest.py    G2 fan-out: countries_id → locations → target sensors
    │                (pm25/pm10/no2) → per-sensor day-window measurements
    ├── gcs.py       raw-zone writer: object naming contract + NDJSON upload
    └── __main__.py  manual CLI entry point
```

One **parameterized** fetcher for any country (guardrail G12 — no per-country
modules). OpenAQ v3 is sensor-centric (G2): there is no flat measurements
endpoint; the client resolves the numeric `countries_id`, pages
`/v3/locations` (whose responses embed each location's sensor list), and
fetches `/v3/sensors/{id}/measurements` per target sensor.

## Raw zone contract (consumed by Phase 3)

```
gs://{bucket}/raw/openaq/{country}/{YYYY-MM-DD}/{sensor_id}.json   measurements
gs://{bucket}/raw/openaq/{country}/{YYYY-MM-DD}/locations.json     sensor→location inventory
```

Object content is newline-delimited **verbatim API page bodies** (G1 —
schema-on-read; no parsing at ingest time). Measurement payloads carry no
sensor/location ids, so identity rides on the object path and the landed
locations pages provide the sensor→location mapping for dbt. Sensors with no
measurements in the window write nothing (no empty objects in the raw zone).

## Running manually

```bash
set -a && source .env && set +a       # OPENAQ_API_KEY, GCS_BUCKET_NAME, GOOGLE_APPLICATION_CREDENTIALS
python -m ingestion.openaq --country AE --date 2026-07-12   # --date defaults to yesterday (UTC)
```

Unit tests (API fully mocked, no network or credentials): `pytest tests/unit/`.
