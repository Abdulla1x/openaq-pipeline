# ingestion/

Python ingestion layer for the OpenAQ v3 API.

## Contents (current)

```
ingestion/
├── constants.py    WHO 2021 air-quality guideline thresholds (source of truth
│                   until the dbt seed supersedes it in Phase 4)
└── openaq/         Package root for the API client (lands in Phase 2)
```

## Planned (Phase 2)

One **parameterized** client that fetches for any country (guardrail G12 — no
per-country modules). OpenAQ v3 is sensor-centric (G2): resolve `countries_id`
via `/v3/countries` → page `/v3/locations?countries_id=...` → enumerate each
location's sensors → fetch `/v3/sensors/{id}/measurements` with a datetime
window + pagination. Raw response JSON is written verbatim to GCS (G1 —
schema-on-read; no parsing at ingest time).
