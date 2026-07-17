# dbt/

dbt transformation project (Phase 4) targeting BigQuery. In Airflow it runs
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

## Seed

`seeds/who_thresholds.csv` — WHO 2021 guideline values
(`pollutant, averaging_period, threshold_value, unit`), the source of truth
for threshold joins (G5). `tests/unit/test_who_seed_sync.py` keeps it in
sync with `ingestion/constants.py`.

## Contents (current)

```
dbt/
├── models/            staging/ intermediate/ mart/ + schema.yml tests per layer
├── seeds/             who_thresholds.csv + schema.yml
├── macros/ tests/ analyses/   empty until needed
├── dbt_project.yml    Project config: name, paths, materializations
├── packages.yml       dbt package dependencies (dbt_utils)
├── package-lock.yml   Pinned package versions (committed, like any lockfile)
└── profiles.yml       Committed BigQuery profile using env_var() only
```

SQL style is enforced by SQLFluff (repo-root `.sqlfluff`; blocking in CI
since Phase 4).
