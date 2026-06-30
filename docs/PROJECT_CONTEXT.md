# PROJECT_CONTEXT.md — OpenAQ Pipeline

> **Document type:** Living source of truth. Version-controlled, updated at the end of every phase.
> **Version:** 1.1 · **Last updated:** 2026-06-30 · **Current phase:** Phase 1 (not started)
> **Canonical location:** `docs/PROJECT_CONTEXT.md` in the repo. Mirror as a reference copy for ongoing design sessions.

---

## 0. How to use this document

This file initializes every phased work session — a design/planning pass, followed by an implementation pass. Read it first, then work only on the **current phase** named above.

Rules for whoever (human or model) edits this file:
- **It is a living doc.** At the end of each phase, update the relevant sections, bump the version, and add a changelog entry (Section 11). Do not let it drift from reality — a stale context file is worse than none.
- **Decisions carry rationale.** When a guardrail changes, record *why*, not just the new state (Section 4). The "why" is the point; the project is a learning exercise as much as a deliverable.
- **Schema marked `[ASSERTED]` is a guess** until real data confirms it. Any phase downstream of ingestion must inspect the actual BigQuery table, not trust this file's asserted schema.
- **One source of truth.** This file lives in the repo so it's read natively during implementation sessions; it is mirrored as a reference copy so design sessions see it without pasting. Edit the repo copy; re-sync the mirror when it changes.

---

## 1. Project identity & goal

**What:** A batch data engineering pipeline ingesting air-quality data from the OpenAQ v3 API, comparing the UAE and Pakistan on PM2.5 / PM10 / NO2 and their WHO-threshold exceedance rates. The cross-country data-quality gap (UAE = sparse-but-instrumented; Pakistan = growing-but-inconsistent coverage) is itself an intended analytical finding.

**Why it exists (career framing — be honest in every session):**
- Built by a 3rd-year CS student (GPA 3.97, Canadian University Dubai, grad ~Dec 2026) for **data-engineering / data-science internship and new-grad recruiting**.
- Honest ceiling: a portfolio project does **not** qualify anyone for mid-to-senior DE roles — those gate on years of production ownership. This project's realistic payoff is making the author a **standout intern / new-grad candidate** who can demonstrate production *patterns* and explain where they'd break.
- The flagship artifact filling the author's audited resume gaps: no cloud, no orchestration, no distributed/ELT tooling, shallow GitHub.

**Deliberate-over-engineering stance (state this in interviews and the README):** the total dataset is a few hundred MB over years. Airflow + GCS + BigQuery + dbt + Looker is overkill on the merits — a single Postgres instance would suffice. The stack is chosen to *demonstrate the production pattern at small scale*. Owning this reads as senior judgment; pretending it's a real production system invites scrutiny it can't survive.

---

## 2. Refined technical stack

| Layer | Technology | Version / note |
|---|---|---|
| Orchestration | Apache Airflow | 2.9.1, **LocalExecutor**, Docker |
| Metadata DB | PostgreSQL | 15 (Airflow-internal) |
| dbt-in-Airflow | **astronomer-cosmos** | renders dbt models as model-level Airflow tasks |
| Data lake | GCP Cloud Storage | raw JSON landing |
| Warehouse | GCP BigQuery | free tier (10GB storage / 1TB query) |
| Transformation | dbt-core + dbt-bigquery | 1.8.3 / 1.8.2 |
| Serving | Looker Studio | native BigQuery connector |
| IaC | Terraform | all GCP resources; remote tfstate in GCS |
| Source API | OpenAQ | **v3** (sensor-centric — see §4) |
| Language | Python | 3.12 |
| CI/CD | GitHub Actions | ruff + sqlfluff + `dbt parse` + pytest on PRs |
| Observability | dbt source freshness + Elementary | |
| Containers | Docker Desktop | 29.5.3, Windows 11 / WSL2 (Ubuntu 24.04) |
| VCS | GitHub | `github.com/Abdulla1x/openaq-pipeline` (public) |

---

## 3. Target architecture (data flow)

```
OpenAQ v3 API  (countries_id → locations → sensors → per-sensor measurements)
   │  Airflow ingest DAG: dynamic task mapping over sensors, retries+backoff
   ▼
GCS raw/  ── verbatim JSON, partitioned by country/date/sensor  [immutable]
   │  GCS→BQ load job (WRITE_APPEND), job.result() blocks
   ▼
BigQuery  openaq_raw.raw_measurements   ── raw JSON column + ingested_at + source_uri
   │  ingest DAG emits an Airflow Dataset  ───────────────┐
   ▼                                                       │ (data-aware schedule)
dbt (Cosmos)  staging (parse JSON, dedup, units)           │
            → intermediate (daily aggregates + completeness)│
            → mart (24h exceedance) + annual (annual mean)  │
   ▲────────── transform DAG triggered by the Dataset ──────┘
   ▼
Looker Studio  ── UAE vs PK trends, exceedance rates, coverage panel
```

---

## 4. Architectural guardrails (decisions + rationale)

Each rule below corrects a flaw found in an earlier draft of this design. Do not regress them.

**G1 — ELT, not ETL (schema-on-read).** Land raw API JSON into a single `raw_payload JSON` column in BigQuery (+ `ingested_at`, `source_uri`). Parse/typecast in dbt staging. *Why:* a typed parse-at-load couples ingestion to the API schema; any v3 field rename then breaks the load. With schema-on-read, a schema change only breaks a dbt model — fixable and re-runnable.

**G2 — OpenAQ v3 is sensor-centric. There is NO flat `/v3/measurements?countries=[...]` endpoint.** The real flow: resolve `countries_id` for AE/PK via `/v3/countries` → page `/v3/locations?countries_id=...` → enumerate each location's sensors (capture parameter + unit) → fetch `/v3/sensors/{id}/measurements` with `datetime_from`/`datetime_to` + pagination. *Why:* the original design's single country-wide call doesn't exist; this is a fan-out over hundreds of sensors. Filter by `countries_id` (numeric), not the `'AE'`/`'PK'` codes. Aggregated `/days` endpoints exist but pulling raw + aggregating in dbt is the deliberate showcase choice.

**G3 — Dynamic task mapping over sensors.** Because of the G2 fan-out, the ingest DAG maps tasks dynamically across the sensor list rather than 3 static tasks. *Why:* correct for the real API shape and a stronger portfolio signal.

**G4 — WRITE_APPEND + dedup in dbt, not WRITE_TRUNCATE-on-partition.** Append to `raw_measurements` with `ingested_at`; dedup in staging via `row_number() over (partition by location_id, parameter, measurement_ts order by ingested_at desc) = 1`. Use a **rolling lookback** (re-fetch the last N days) to catch late arrivals. *Why:* TRUNCATE-on-partition silently loses data on partial API responses and never picks up back-dated readings; it also violates the immutable-raw principle.

**G5 — Corrected WHO 2021 thresholds, stored as a dbt seed.** The original design's numbers were wrong (mixed 2005/2021 and PM2.5/PM10). Correct 2021 values:

| Pollutant | Annual mean | 24-hour mean | Notes |
|---|---|---|---|
| PM2.5 | 5 µg/m³ | 15 µg/m³ | |
| PM10 | 15 µg/m³ | 45 µg/m³ | |
| NO2 | 10 µg/m³ | 25 µg/m³ | |
| SO2 | — | 40 µg/m³ | no 2021 annual |
| O3 | 60 µg/m³ (peak season) | 100 µg/m³ (8-hour) | |
| CO | — | **4 mg/m³** | unit is mg/m³, not µg/m³ |

Store as a seed with columns `(pollutant, averaging_period, threshold_value, unit)`. *Why:* seeds are versioned, testable, and documented; hardcoded CASE statements are a smell. Source: WHO 2021 Global Air Quality Guidelines.

**G6 — Grain discipline.** A **daily** average may only be compared to a **24-hour** threshold. The mart (`mart_country_compare`) uses 24h thresholds. Annual-mean thresholds require a **separate annual aggregate model**. *Why:* comparing a single day's mean to an annual guideline is a unit/grain error and statistically meaningless.

**G7 — Completeness is a dimension, never a silent filter.** Compute `reading_count` and `hours_covered` per station-day and expose them as columns; do not drop low-coverage days. *Why:* a "fewer than N readings" filter removes disproportionately more Pakistani days, biasing the exact UAE-vs-PK comparison the project exists to make. Surface the coverage gap as a finding instead. Also count *distinct hours covered*, not raw readings (sensors report at different frequencies).

**G8 — exceedance_rate needs an explicit denominator.** `days_exceeded / days_with_data × 100`. State the denominator; it differs sharply between the two countries.

**G9 — dbt via Cosmos; ingest→transform via Airflow Datasets.** Cosmos gives model-level tasks (granular retries/observability) instead of an opaque `dbt run` BashOperator. The ingest DAG produces a Dataset; the transform DAG is scheduled on it (not `TriggerDagRunOperator`). *Why:* Airflow 2.9 native pattern, cleaner, current.

**G10 — Auth & secrets.** GCP operators use an **Airflow Connection** via `gcp_conn_id`; direct dbt/bigquery client calls may use `GOOGLE_APPLICATION_CREDENTIALS`. dbt `profiles.yml` references `{{ env_var(...) }}` (commit it — it holds no secrets). `.env`, key JSON, and `profiles.yml` with real values are gitignored. **Rotate the Fernet key that leaked into the earlier spec doc; never store live secret values in this file.**

**G11 — IaC for all GCP resources.** Terraform provisions bucket, datasets, service account, IAM; remote `tfstate` in a GCS backend.

**G12 — Engineering hygiene as a first-class deliverable.** Conventional commits; feature branches + PRs (not direct-to-main); CI green on every PR; a handful of pytest unit tests on the ingestion client (mock the API). One parameterized fetcher, not per-country files (DRY).

---

## 5. BigQuery layout (planned)

```
openaq_raw
  └── raw_measurements        raw_payload JSON, ingested_at TIMESTAMP, source_uri STRING   [ASSERTED]
openaq_dbt
  ├── stg_measurements        view  — parse JSON, dedup, unit-standardize
  ├── int_daily_aqi           table — daily_avg/max/min, reading_count, hours_covered
  ├── mart_country_compare    table — 24h exceedance flags, exceedance_rate, rolling_7d_avg
  └── mart_annual_compare     table — annual-mean vs annual thresholds
seed: who_thresholds          (pollutant, averaging_period, threshold_value, unit)
```
GCS: `gs://{bucket}/raw/openaq/{country}/{YYYY-MM-DD}/{sensor_id}.json`

---

## 6. Phase roadmap & exit criteria

Phase boundaries fall on **stable interfaces**, not feature counts. One phase ≈ one focused session.

| Phase | Goal | Exit criterion (testable) |
|---|---|---|
| **0 — Hygiene & CI/CD** | Repo reads professional; living context doc committed | PR shows CI (ruff/sqlfluff/dbt parse/pytest) green; main protected; no empty scaffold files; Fernet key rotated |
| **1 — Cloud IaC** | Provision all GCP via Terraform | `terraform apply` idempotent (2nd apply = no changes); SA writes to GCS + creates a BQ table; tfstate in GCS backend |
| **2 — Ingestion** | Tested v3 client → GCS raw JSON | Unit tests green; manual run lands real raw JSON for 1 day/1 country; empty responses don't crash |
| **3 — Orchestration** | Airflow DAG: dynamic mapping + raw load | DAG green end-to-end 1 day; BQ counts reconcile vs API; same-day rerun safe (append); Dataset emitted |
| **4 — Transformation** | dbt ELT via Cosmos | dbt run+test green; 1 exceedance flag hand-verified; Cosmos per-model tasks; transform DAG triggers off Dataset |
| **5 — Backfill + observability** | Load 1–2 yrs; make it observable | History validated (no unexplained gaps); Elementary report; 1 integration test green |
| **6 — Serving** | Looker Studio dashboard + finding | Dashboard live; one-sentence finding written (who exceeds more, by how much, caveated by coverage) |
| **7 — Polish** | README sells the repo | A stranger can run it from the README; finding is front-and-center; architecture diagram present |

Interface contracts between phases: GCS raw layout (2→3), `raw_measurements` table (3→4), mart tables (4→6).

---

## 7. Current state (snapshot — update each phase)

**Phase 0 — complete (merged to main as commit 700fe1a, 2026-06-30).**

Done:
- All empty scaffold files removed (Python stubs, Terraform stubs, dbt schema stubs, per-country fetchers that violated G12). `.gitkeep` placeholders added where directories must persist empty.
- Fernet key rotated; old leaked key no longer in `.env`.
- `pyproject.toml` configured (ruff + pytest, `pythonpath = ["."]` for test imports without an editable install).
- `Makefile` with `up`/`down`/`logs`/`lint`/`test` targets.
- dbt minimal config: `dbt_project.yml`, `dbt/profiles.yml` (committed — uses `{{ env_var() }}` only, no secrets, per G10), `dbt/packages.yml` (dbt_utils).
- `scripts/bootstrap.sh` written (checks docker/git/python3, copies `.env.example`).
- `ingestion/constants.py` — WHO 2021 thresholds (G5) as the Phase 0-3 source of truth, with 5 passing unit tests in `tests/unit/test_who_constants.py`. Will be superseded by a dbt seed in Phase 4 — keep both in sync until then.
- `docs/architecture.md` — concise architecture summary distinct from this file.
- `.github/workflows/ci.yml` — three jobs (`lint`, `dbt-parse`, `pytest`), all passing on PR #1.
- This file committed to the repo for the first time at `docs/PROJECT_CONTEXT.md` (previously existed only as a personal planning document, never in version control).
- Branch protection (GitHub Ruleset, not Classic) active on `main`: requires PR, requires `lint`+`dbt-parse`+`pytest` to pass, blocks force pushes, no bypass.
- **Commit attribution.** Disabled automatic commit co-authorship trailers going forward; one local commit amended to remove a trailer that had already been added before push. Pre-existing pushed commits on main predate this and were never affected.

**Phase 0 exit criteria — verified:**
- [x] CI green (ruff, sqlfluff, dbt parse, pytest) — confirmed on PR #1, all 3 jobs passed
- [x] Branch protection on main active and enforcing (confirmed: PR could not show "Ready to merge" until ruleset was properly configured with Active status + target branch + required checks)
- [x] No empty scaffold files
- [x] Fernet key rotated

**Not started:** everything in §6 Phases 1–7. No ingestion, DAG, dbt models, GCP resources, or dashboard exist yet.

**Known liabilities carried forward:** the "remove CI workflows" commit remains in history (6524216) — not rewritten, just superseded; the forked `CourseScraping-BU` repo (remove/rebuild if referenced) — not addressed in Phase 0, still open.

## 7.5 Deviations and discoveries during Phase 0 (for institutional memory)

- **Local Python version drift.** Host `python3` resolves to 3.14.4 (confirmed via `.venv` creation), while `pyproject.toml`'s `requires-python`, the Airflow Docker image, and this document's §2 stack table all target 3.12. Not yet a problem (Phase 0 code has no version-specific behavior), but will need resolving before Phase 2 ingestion code is written — either pin a 3.12 venv explicitly or confirm 3.14 compatibility for all Phase 2+ dependencies (especially `apache-airflow-providers-google`, which has historically had narrow pandas/Python version constraints — see the pandas conflict in the original build).
- **`sqlfluff` is currently a no-op.** `continue-on-error: true` on the SQLFluff CI step was added defensively, but empirically (tested locally) SQLFluff exits 0 on an empty `dbt/models/` directory regardless. The flag has zero effect today. It becomes load-bearing in Phase 4 when real `.sql` files land — at that point, decide explicitly whether lint failures should block merges (remove the flag) or only warn (keep it, but make that a deliberate choice, not inherited inertia).
- **GitHub Rulesets, not Classic branch protection.** Used the newer Rulesets UI instead of Classic. Functionally equivalent for our needs (require PR, require status checks, block force push) but the setup flow is non-obvious — a new ruleset defaults to Enforcement: Disabled and no target branch, both of which must be explicitly set or the rule silently does nothing while looking configured.
- **Required-check names matter for branch protection.** Two CI jobs were initially both named `test` (intended to simplify required-checks down to one name); this was a real bug, not a style choice — GitHub's Checks API keys on the job's `name:` field, and whichever job reports last silently overwrites the other's status, making branch protection non-deterministic. Caught before merge; fixed to `lint`/`dbt-parse`/`pytest`.

---

## 8. Genuinely open questions

- **OpenAQ v3 free-tier rate limits** — unknown exact limits; the per-sensor fan-out makes this load-bearing for both daily runs and backfill. Confirm and implement backoff before Phase 5.
- **Backfill volume** — hundreds of sensors × 1–2 years × pagination = heavy. Chunk by date window and/or sensor batch.
- **Verified vs asserted schema** — `raw_measurements` and the parsed staging columns are `[ASSERTED]` until Phase 2/3 land real data.

---

## 9. Working preferences

- Guide step by step with the "why"; do not dump a finished codebase.
- Brutally honest; surface tradeoffs; state assumptions; push back when warranted; no sycophancy or filler.
- Implementation sessions = scaffolding/writing/debugging in the terminal. Design sessions = planning/architecture/learning.
- Prefer editing over rewriting whole files. Keep solutions simple and direct.

---

## 10. Source-of-truth facts (verified)

- WHO thresholds in §4/G5 are the **2021** Global Air Quality Guidelines (verified 2026-06-18).
- OpenAQ **v3** is sensor-centric per §4/G2 (verified against OpenAQ docs 2026-06-18): `countries → locations → sensors → measurements`, country filtered by `countries_id`.

---

## 11. Changelog

| Version | Date | Change |
|---|---|---|
| 1.0 | 2026-06-18 | Initial context doc. Captures architectural review corrections (G1–G12), phase roadmap, and career framing. |
| 1.1 | 2026-06-30 | Phase 0 complete (merged main as 700fe1a). CI green, branch protection active, all scaffolds resolved, Fernet key rotated. Added §7.5 documenting Python version drift (host 3.14 vs target 3.12), sqlfluff no-op status until Phase 4, and the duplicate-job-name branch-protection bug caught pre-merge. |
