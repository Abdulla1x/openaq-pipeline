# Architecture

## Overview

A batch data engineering pipeline that ingests air-quality measurements from the
OpenAQ v3 API, compares the UAE and Pakistan on PM2.5 / PM10 / NO2 against WHO
2021 thresholds, and surfaces the cross-country data-quality gap as an explicit
finding rather than hiding it behind a completeness filter.

## Data flow

```
OpenAQ v3 API (countries → locations → sensors → measurements)
   │  Airflow: dynamic task mapping over sensors
   ▼
GCS raw/  — verbatim JSON, partitioned by country/date/sensor [immutable]
   │  GCS → BigQuery load (WRITE_APPEND)
   ▼
BigQuery openaq_raw.raw_measurements — raw JSON + ingested_at + source_uri
   │  dbt (via Cosmos), triggered off an Airflow Dataset
   ▼
staging → intermediate (daily aggregates + completeness) → mart (exceedance)
   ▼
Looker Studio — UAE vs PK trends, exceedance rates, coverage panel
```

## Why this stack

The dataset is a few hundred MB over multiple years — a single Postgres instance
would technically suffice. Airflow + GCS + BigQuery + dbt + Looker is deliberate
over-engineering: the project exists to demonstrate production data-engineering
patterns at small scale as a portfolio exercise, not to solve a scale problem
that doesn't exist. See PROJECT_CONTEXT.md §1 for the full framing.

## Key design decisions

See PROJECT_CONTEXT.md §4 (architectural guardrails G1–G12) for the full list
with rationale. The most consequential:

- **Schema-on-read (G1):** raw API JSON lands in a single JSON column; typing
  and parsing happen in dbt staging, not at load time.
- **OpenAQ v3 is sensor-centric, not country-flat (G2):** there is no single
  "give me all UAE measurements" endpoint. Ingestion fans out across hundreds
  of per-sensor calls.
- **Completeness is a dimension, not a filter (G7):** station-days with sparse
  readings are kept and labeled, not dropped — dropping them would bias the
  UAE-vs-Pakistan comparison the project exists to make.
- **WHO 2021 thresholds as a dbt seed (G5):** versioned and testable instead of
  hardcoded in SQL.

## Full documentation

The living source of truth for this project — phase roadmap, full guardrail
rationale, and current state — is `docs/PROJECT_CONTEXT.md`.
