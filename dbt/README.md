# dbt/

dbt transformation project with a three-layer model architecture targeting
BigQuery. Models land in Phase 4; today only the project config exists.

## Connection profile

`profiles.yml` **is committed** — it contains only `{{ env_var(...) }}`
references, no secrets (guardrail G10). Set `GCP_PROJECT_ID`,
`BIGQUERY_DATASET`, and `GOOGLE_APPLICATION_CREDENTIALS` in your environment
(or `.env`) and dbt resolves them at runtime. Do not create a local copy with
real values inside the repo.

## Layer responsibilities (Phase 4)

| Layer | Directory | Purpose |
|---|---|---|
| Staging | `models/staging/` | Parse raw JSON, dedup, standardize units (views) |
| Intermediate | `models/intermediate/` | Daily aggregates + completeness dimensions (`reading_count`, `hours_covered` — exposed as columns, never a silent filter, per G7) |
| Mart | `models/mart/` | 24h exceedance vs WHO thresholds, plus a separate annual-mean model (grain discipline, G6) |

## Contents (current)

```
dbt/
├── models/           staging/ intermediate/ mart/ — empty until Phase 4
├── seeds/            who_thresholds seed lands in Phase 4 (G5)
├── macros/ tests/ analyses/   empty until needed
├── dbt_project.yml   Project config: name, paths, materializations
├── packages.yml      dbt package dependencies (dbt_utils)
└── profiles.yml      Committed BigQuery profile using env_var() only
```
