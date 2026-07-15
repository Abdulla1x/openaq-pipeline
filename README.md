# openaq-pipeline

Batch data engineering pipeline ingesting air-quality data from the OpenAQ v3
API, comparing the UAE and Pakistan on PM2.5 / PM10 / NO2 against WHO 2021
thresholds. The cross-country data-quality gap is itself an intended finding.

**Status:** Phases 0–3 complete — repo hygiene + CI/CD, GCP infrastructure via
Terraform, the tested OpenAQ v3 ingestion client, and the Airflow ingest DAG
(dynamic task mapping over sensors) loading verbatim raw JSON into BigQuery
with count reconciliation. Phase 4 (dbt transformation via Cosmos) is next;
Phases 5–7 (backfill, serving, polish) not yet started — see
`docs/PROJECT_CONTEXT.md` §6 for the roadmap and exit criteria.

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python → OpenAQ v3 API (sensor-centric fan-out) |
| Raw storage | GCP Cloud Storage (verbatim JSON) |
| Warehouse | BigQuery |
| Transformation | dbt (staging → intermediate → mart) |
| Orchestration | Apache Airflow 2.9 (Docker Compose, LocalExecutor) |
| IaC | Terraform |
| Dashboard | Looker Studio |

## Repository layout

```
.
├── airflow/        Airflow Docker image + the openaq_ingest DAG
├── dbt/            dbt project config; models/seeds land in Phase 4
├── ingestion/      OpenAQ v3 client (fan-out to GCS raw) + WHO threshold constants
├── infra/          Terraform IaC for GCP (bucket, datasets, service account)
├── scripts/        Dev utility scripts (bootstrap)
├── tests/          Pytest suite (unit now, integration in Phase 5)
├── docs/           PROJECT_CONTEXT.md (source of truth) + architecture overview
└── looker/         Looker Studio exports; lands in Phase 6
```

## Quickstart (local dev environment only)

```bash
bash scripts/bootstrap.sh     # checks prerequisites, creates .env from template
# fill in the placeholder values in .env
make up                       # start Airflow at http://localhost:8080
make lint && make test        # ruff + pytest
```

GCP resources are provisioned exclusively via Terraform (`infra/`) — no
script creates cloud resources.

## Documentation

`docs/PROJECT_CONTEXT.md` is the living source of truth: architectural
guardrails with rationale, phase roadmap, and current state.
`docs/architecture.md` is the short-form overview.
