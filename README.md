# openaq-pipeline

Production-grade data engineering pipeline ingesting air quality data from the
OpenAQ API for UAE and Pakistan.

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python → OpenAQ v3 API |
| Raw storage | GCP Cloud Storage (JSON) |
| Warehouse | BigQuery |
| Transformation | dbt (staging → intermediate → mart) |
| Orchestration | Apache Airflow (Docker Compose) |
| Dashboard | Looker Studio |

## Repository layout

```
.
├── airflow/        Airflow DAGs, plugins, and Docker config
├── dbt/            dbt transformation project (3-layer model)
├── ingestion/      Python OpenAQ API client and country fetchers
├── infra/          Terraform IaC for GCS and BigQuery
├── scripts/        Dev utility scripts (bootstrap, backfill)
├── tests/          Python unit and integration tests
├── docs/           Architecture diagrams and runbooks
└── looker/         Looker Studio dashboard exports
```

## Quickstart

```bash
cp .env.example .env          # fill in your credentials
bash scripts/bootstrap.sh     # provision GCP resources + init Airflow
docker compose up -d          # start Airflow
```
