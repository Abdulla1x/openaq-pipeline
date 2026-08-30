# Architecture

## Overview

A batch data engineering pipeline that ingests air-quality measurements from the
OpenAQ v3 API, compares the UAE and Pakistan on PM2.5 / PM10 / NO2 against WHO
2021 thresholds, and surfaces the cross-country data-quality gap as an explicit
finding rather than hiding it behind a completeness filter.

## Data flow

The rendered architecture diagram lives in the [root README](../README.md#architecture)
— one source, so it cannot drift from a second copy here. In one line:

OpenAQ v3 API → Airflow ingest DAG (dynamic task mapping over sensors, 7-day
rolling lookback) → GCS raw zone (verbatim JSON, immutable) → BigQuery
`openaq_raw.raw_measurements` → *Airflow Dataset event* → dbt via Cosmos
(staging → `int_daily_aqi` → marts) → Looker Studio.

The backfill CLI enters the same GCS and BigQuery load contract as the DAG
(`ingestion/openaq/bq_load.py`), so the two ingestion paths cannot drift apart.

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
- **Rolling 7-day lookback (G4):** the daily DAG re-fetches the last week per
  sensor at no extra request cost; late or corrected readings win the
  latest-`ingested_at` dedup in staging.

## History and observability (Phase 5)

- **Backfill CLI** (`python -m ingestion.openaq backfill`): chunked, resumable
  wide-window history load (AE from 2024-07, PK from 2025-06 — spans chosen
  from sensor metadata). It shares the DAG's exact BigQuery load contract via
  `ingestion/openaq/bq_load.py`, so the two load paths cannot drift; every
  chunk is count-reconciled against the API before being checkpointed.
- **Observability:** dbt source freshness on `raw_measurements.ingested_at`
  (`make freshness`) plus the Elementary dbt package writing run/test metadata
  to a dedicated `openaq_dbt_elementary` dataset, with an HTML report via the
  edr CLI (`make elementary-report`).

## Full documentation

The living source of truth for this project — phase roadmap, full guardrail
rationale, and current state — is `docs/PROJECT_CONTEXT.md`.
