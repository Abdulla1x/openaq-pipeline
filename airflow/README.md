# airflow/

Airflow Docker configuration and DAGs for pipeline orchestration.

## Contents (current)

```
airflow/
├── dags/
│   └── openaq_ingest.py  Daily raw-ingestion DAG (Phase 3)
├── plugins/operators/    Empty — no custom operators needed so far
├── Dockerfile            Airflow 2.9.1 image + project deps, installed under
│                         the official constraints for reproducibility
└── requirements.txt      Python packages baked into the Airflow image
```

Runtime logs are written to the repo-root `logs/` directory (gitignored,
mounted as a Docker volume by `docker-compose.yml`).

## The ingest DAG (`openaq_ingest`)

Daily at 02:00 UTC, for the previous (completed) UTC day; per country (AE, PK):

1. `prepare_country_run` — resolves the country, lands the verbatim
   `locations.json` inventory in GCS, emits the target-sensor list.
2. `fetch_sensor` — **dynamically mapped** over that list (G3), one task per
   sensor, throttled by the `openaq_api` pool (4 slots vs the 60 req/min API
   limit). Per-sensor failures return `status="failed"` instead of raising.
3. `summarize_country` — aggregates outcomes; fails only if >20% of sensor
   fetches failed (a handful of persistently broken PK sensors is normal).
4. `ensure_raw_table` + `load_raw_to_bq` — appends verbatim page bodies into
   `openaq_raw.raw_measurements (raw_payload JSON, ingested_at, source_uri)`
   via a temp external table; `_FILE_NAME` becomes `source_uri` (G1/G4).
   Emits the `bigquery://…/raw_measurements` **Dataset** that will schedule
   the Phase 4 transform DAG (G9).
5. `reconcile_counts` — asserts API-run measurement totals equal what the
   latest batch landed in BigQuery.

DAG structure is tested by `tests/dags/test_dag_integrity.py` (the
`dag-validate` CI job); `make dag-test` runs a quick import check inside the
container.

## Planned (Phase 4)

A transform DAG scheduled on the raw-measurements Dataset, running dbt via
astronomer-cosmos (G9).
