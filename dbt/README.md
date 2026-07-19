# dbt/

dbt transformation project (Phase 4; observability added in Phase 5)
targeting BigQuery. In Airflow it runs
via astronomer-cosmos as one task per dbt node (`openaq_transform` DAG,
scheduled on the raw_measurements Dataset — G9); locally it runs with plain
`dbt build` given the env vars below.

## Connection profile

`profiles.yml` **is committed** — it contains only `{{ env_var(...) }}`
references, no secrets (guardrail G10). Set `GCP_PROJECT_ID`,
`BIGQUERY_DATASET`, and `GOOGLE_APPLICATION_CREDENTIALS` in your environment
(or `.env`) and dbt resolves them at runtime. Do not create a local copy with
real values inside the repo.

## Model layers

| Layer | Models | Purpose |
|---|---|---|
| Staging (views) | `stg_measurements`, `stg_locations`, `stg_sensors` | Parse raw JSON pages (G1), dedup append batches (G4). Measurement payloads carry no ids — sensor/country identity is parsed from `source_uri`; the sensor→location bridge comes from the landed locations inventory |
| Intermediate (table) | `int_daily_aqi` | Station-day aggregates with completeness as columns (`reading_count`, `hours_covered`) — never a silent filter (G7) |
| Mart (tables) | `mart_country_compare`, `mart_annual_compare` | Daily vs 24h thresholds and annual vs annual thresholds, kept in separate models (grain discipline, G6); exceedance rates carry explicit denominators (G8) |

## Seeds

- `seeds/who_thresholds.csv` — WHO 2021 guideline values
  (`pollutant, averaging_period, threshold_value, unit`), the source of truth
  for threshold joins (G5). `tests/unit/test_who_seed_sync.py` keeps it in
  sync with `ingestion/constants.py`.
- `seeds/known_bad_sensors.csv` — persistently-500ing sensors observed live
  (58 rows, all PK, with first/last-observed dates). Consumed by the backfill
  CLI's `--skip-sensors-csv` so known-broken sensors don't burn retry budget.

## Observability (Phase 5)

- **Source freshness** on `raw_measurements.ingested_at`
  (warn 30h / error 54h, configured in `models/staging/sources.yml`):
  `make freshness`. Deliberately not a task in the Dataset-triggered
  transform DAG — that DAG only runs when data just arrived.
- **Elementary** (dbt package 0.16.4): on-run-end hooks record run/test
  results into the Terraform-provisioned `openaq_dbt_elementary` dataset
  (kept out of the dataset Looker browses). One-time table bootstrap:
  `make elementary-bootstrap`. The HTML report needs the edr CLI in its own
  venv (`.venv-edr/` — its dep tree conflicts with the pinned dev venv and
  the Airflow image): `make elementary-report` writes the gitignored
  `elementary_report.html`. Elementary's models are excluded from the cosmos
  render so the transform DAG stays one task per pipeline node.

## Contents (current)

```
dbt/
├── models/            staging/ intermediate/ mart/ + schema.yml tests per layer
├── seeds/             who_thresholds.csv, known_bad_sensors.csv + schema.yml
├── analyses/          history_gap_audit.sql — Phase 5 backfill gap classification
├── macros/ tests/     empty until needed
├── dbt_project.yml    Project config: name, paths, materializations
├── packages.yml       dbt package dependencies (dbt_utils, elementary)
├── package-lock.yml   Pinned package versions (committed, like any lockfile)
└── profiles.yml       Committed BigQuery profile using env_var() only
```

SQL style is enforced by SQLFluff (repo-root `.sqlfluff`; blocking in CI
since Phase 4).
