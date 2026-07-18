# openaq-pipeline

Batch data engineering pipeline ingesting air-quality data from the OpenAQ v3
API, comparing the UAE and Pakistan on PM2.5 / PM10 / NO2 against WHO 2021
thresholds. The cross-country data-quality gap is itself an intended finding.

**Status:** Phases 0–5 complete — repo hygiene + CI/CD, GCP infrastructure via
Terraform, the tested OpenAQ v3 ingestion client, the Airflow ingest DAG
(dynamic task mapping over sensors, 7-day rolling lookback) loading verbatim
raw JSON into BigQuery, the dbt ELT layer (staging → daily aggregates →
WHO-exceedance marts) run by astronomer-cosmos as per-model Airflow tasks on
a Dataset-triggered transform DAG, and now the backfilled history (1.6M
measurements: AE from 2024-07, PK from 2025-06 — spans chosen from sensor
metadata, every chunk count-reconciled, gaps audited and classified) with
observability via dbt source freshness + Elementary. Phases 6–7 (serving,
polish) remain — see `docs/PROJECT_CONTEXT.md` §6 for the roadmap.

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python → OpenAQ v3 API (sensor-centric fan-out) |
| Raw storage | GCP Cloud Storage (verbatim JSON) |
| Warehouse | BigQuery |
| Transformation | dbt (staging → intermediate → mart) |
| Orchestration | Apache Airflow 2.9 (Docker Compose, LocalExecutor) |
| Observability | dbt source freshness + Elementary |
| IaC | Terraform |
| Dashboard | Looker Studio |

## Repository layout

```
.
├── airflow/        Airflow Docker image + the openaq_ingest / openaq_transform DAGs
├── dbt/            dbt project: staging/intermediate/mart models + WHO-thresholds seed
├── ingestion/      OpenAQ v3 client (fan-out to GCS raw) + WHO threshold constants
├── infra/          Terraform IaC for GCP (bucket, datasets, service account)
├── scripts/        Dev utility scripts (bootstrap)
├── tests/          Pytest suite (unit + DAG integrity + live integration test)
├── docs/           PROJECT_CONTEXT.md (source of truth) + architecture overview
└── looker/         Looker Studio exports; lands in Phase 6
```

## Quickstart (local dev environment only)

```bash
bash scripts/bootstrap.sh     # checks prerequisites, creates .env from template
# fill in the placeholder values in .env
make up                       # start Airflow at http://localhost:8080
make lint && make test        # ruff + pytest (unit suite)
make dag-test                 # DAG import check inside the Airflow container
```

CI runs five checks on every PR (`lint`, `dbt-parse`, `pytest`, `terraform`,
`dag-validate`); all are required by branch protection on `main`.

GCP resources are provisioned exclusively via Terraform (`infra/`) — no
script creates cloud resources.

## Documentation

`docs/PROJECT_CONTEXT.md` is the living source of truth: architectural
guardrails with rationale, phase roadmap, and current state.
`docs/architecture.md` is the short-form overview.
