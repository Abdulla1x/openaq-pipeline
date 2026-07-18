# PROJECT_CONTEXT.md — OpenAQ Pipeline

> **Document type:** Living source of truth. Version-controlled, updated at the end of every phase.
> **Version:** 1.10 · **Last updated:** 2026-07-18 · **Current phase:** Phase 5 (not started)
> **Canonical location:** `docs/PROJECT_CONTEXT.md` in the repo.

---

## 0. How to use this document

Maintenance rules for this file:
- **It is a living doc.** At the end of each phase, update the relevant sections, bump the version, and add a changelog entry (Section 10). Do not let it drift from reality — a stale context file is worse than none.
- **Decisions carry rationale.** When a guardrail changes, record *why*, not just the new state (Section 4). The "why" is the point; the project is a learning exercise as much as a deliverable.
- **Schema marked `[ASSERTED]` is a guess** until real data confirms it. Any phase downstream of ingestion must inspect the actual BigQuery table, not trust this file's asserted schema.

---

## 1. Project identity & goal

**What:** A batch data engineering pipeline ingesting air-quality data from the OpenAQ v3 API, comparing the UAE and Pakistan on PM2.5 / PM10 / NO2 and their WHO-threshold exceedance rates. The cross-country data-quality gap (UAE = sparse-but-instrumented; Pakistan = growing-but-inconsistent coverage) is itself an intended analytical finding.

**Why it exists:** a portfolio/learning project built to demonstrate production data-engineering patterns end-to-end — cloud IaC, orchestration, ELT, testing, CI/CD — at deliberately small scale, and to explain where each pattern would break at real scale.

**Deliberate-over-engineering stance (owned in the README):** the total dataset is a few hundred MB over years. Airflow + GCS + BigQuery + dbt + Looker is overkill on the merits — a single Postgres instance would suffice. The stack is chosen to *demonstrate the production pattern at small scale*; that tradeoff is stated openly rather than presented as a scale requirement that doesn't exist.

---

## 2. Refined technical stack

| Layer | Technology | Version / note |
|---|---|---|
| Orchestration | Apache Airflow | 2.9.1, **LocalExecutor**, Docker |
| Metadata DB | PostgreSQL | 15 (Airflow-internal) |
| dbt-in-Airflow | **astronomer-cosmos** | 1.15.0 — one Airflow task per dbt node (model run + test pairs) |
| Data lake | GCP Cloud Storage | raw JSON landing |
| Warehouse | GCP BigQuery | free tier (10GB storage / 1TB query) |
| Transformation | dbt-core + dbt-bigquery | 1.8.3 / 1.8.2 |
| Serving | Looker Studio | native BigQuery connector |
| IaC | Terraform | 1.15.8, google provider ~> 7.0; remote tfstate in GCS |
| Source API | OpenAQ | **v3** (sensor-centric — see §4) |
| Language | Python | 3.12 |
| CI/CD | GitHub Actions | 5 jobs on PRs: `lint` (ruff+sqlfluff), `dbt-parse`, `pytest`, `terraform`, `dag-validate` |
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

**G8 — exceedance_rate needs an explicit denominator.** The principle: never publish a rate without exposing its denominator as a column — it differs sharply between the two countries. **Realized form (Phase 4; recorded in the pre-Phase-5 audit):** the daily mart's rate is the *share of reporting stations exceeding that day* (`locations_exceeding / locations_comparable × 100`, both exposed; a station-day with no matching threshold leaves the denominator, never the table). The originally-drafted days-based rate (`days_exceeded / days_with_data`) is implemented nowhere yet — deliberately deferred to Phase 5/6, when backfill gives it a history to be a rate *of* (against today's ~2 ingested days it is statistically empty).

**G9 — dbt via Cosmos; ingest→transform via Airflow Datasets.** Cosmos gives model-level tasks (granular retries/observability) instead of an opaque `dbt run` BashOperator. The ingest DAG produces a Dataset; the transform DAG is scheduled on it (not `TriggerDagRunOperator`). *Why:* Airflow 2.9 native pattern, cleaner, current.

**G10 — Auth & secrets.** GCP operators use an **Airflow Connection** via `gcp_conn_id`; direct dbt/bigquery client calls may use `GOOGLE_APPLICATION_CREDENTIALS`. dbt `profiles.yml` references `{{ env_var(...) }}` (commit it — it holds no secrets). `.env`, key JSON, and `profiles.yml` with real values are gitignored. **Never store live secret values in this file; rotate any credential that touches an unprotected surface.**

**G11 — IaC for all GCP resources.** Terraform provisions bucket, datasets, service account, IAM; remote `tfstate` in a GCS backend.

**G12 — Engineering hygiene as a first-class deliverable.** Conventional commits; feature branches + PRs (not direct-to-main); CI green on every PR; a handful of pytest unit tests on the ingestion client (mock the API). One parameterized fetcher, not per-country files (DRY).

---

## 5. BigQuery layout (planned)

```
openaq_raw
  └── raw_measurements        raw_payload JSON, ingested_at TIMESTAMP, source_uri STRING
                              partitioned by DATE(ingested_at)   [VERIFIED 2026-07-15, live load]
openaq_dbt                    [VERIFIED 2026-07-17, dbt build against live BQ — 58/58 green]
  ├── stg_measurements        view  — UNNEST pages, parse ids from source_uri, dedup (G4)
  ├── stg_locations           view  — station inventory from locations.json pages
  ├── stg_sensors             view  — sensor→location bridge (embedded sensors[] arrays)
  ├── int_daily_aqi           table — grain (location, parameter, day): daily_avg/min/max,
  │                                   reading_count, hours_covered (G7)
  ├── mart_country_compare    table — grain (country, parameter, day): 24h exceedance,
  │                                   explicit denominators (G8), rolling_7d_avg
  └── mart_annual_compare     table — grain (country, parameter, year) vs annual thresholds (G6)
seed: who_thresholds          (pollutant, averaging_period, threshold_value, unit)
```
The two extra staging views (vs the original single-view plan) exist because
measurement payloads carry no ids (§7.5): identity and the sensor→location
join both come from the landed locations pages.
GCS **[VERIFIED 2026-07-13, real data landed]**:
```
gs://{bucket}/raw/openaq/{country}/{YYYY-MM-DD}/{sensor_id}.json   per-sensor measurements
gs://{bucket}/raw/openaq/{country}/{YYYY-MM-DD}/locations.json     verbatim locations pages (sensor→location inventory)
```
Object content is NDJSON: one line per **verbatim API page body** (G1). The
`locations.json` addition exists because measurement payloads carry no
sensor/location ids (verified against live API) — identity rides on the object
path, and dbt needs the landed locations pages for the sensor→location join.
Sensors with zero measurements in the window write no object.

The Phase 3 load reads each day prefix with a `/*` wildcard, so **the
`locations.json` pages land in `raw_measurements` too** (one row per page,
identifiable by `source_uri`). Deliberate: Phase 4's staging gets the
sensor→location inventory straight from BigQuery — no second load path.
`raw_measurements` rows are *pages*, not measurements; staging must UNNEST
`raw_payload.results` and parse sensor/country/day identity out of
`source_uri`.

---

## 6. Phase roadmap & exit criteria

Phase boundaries fall on **stable interfaces**, not feature counts. One phase ≈ one focused session.

| Phase | Goal | Exit criterion (testable) |
|---|---|---|
| **0 — Hygiene & CI/CD** | Repo reads professional; living context doc committed | PR shows CI (ruff/sqlfluff/dbt parse/pytest) green; main protected; no empty scaffold files; secrets rotated |
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
- Secrets hygiene pass: Airflow Fernet key rotated; `.env` holds only current values and stays gitignored.
- `pyproject.toml` configured (ruff + pytest, `pythonpath = ["."]` for test imports without an editable install).
- `Makefile` with `up`/`down`/`logs`/`lint`/`test` targets.
- dbt minimal config: `dbt_project.yml`, `dbt/profiles.yml` (committed — uses `{{ env_var() }}` only, no secrets, per G10), `dbt/packages.yml` (dbt_utils).
- `scripts/bootstrap.sh` written (checks docker/git/python3, copies `.env.example`).
- `ingestion/constants.py` — WHO 2021 thresholds (G5) as the Phase 0-3 source of truth, with 5 passing unit tests in `tests/unit/test_who_constants.py`. Will be superseded by a dbt seed in Phase 4 — keep both in sync until then.
- `docs/architecture.md` — concise architecture summary distinct from this file.
- `.github/workflows/ci.yml` — three jobs (`lint`, `dbt-parse`, `pytest`), all passing on PR #1.
- This file committed to the repo for the first time at `docs/PROJECT_CONTEXT.md` (previously existed only as a personal planning document, never in version control).
- Branch protection (GitHub Ruleset, not Classic) active on `main`: requires PR, requires `lint`+`dbt-parse`+`pytest` to pass, blocks force pushes, no bypass.

**Phase 0 exit criteria — verified:**
- [x] CI green (ruff, sqlfluff, dbt parse, pytest) — confirmed on PR #1, all 3 jobs passed
- [x] Branch protection on main active and enforcing (confirmed: PR could not show "Ready to merge" until ruleset was properly configured with Active status + target branch + required checks)
- [x] No empty scaffold files
- [x] Secrets rotation completed

**Phase 1 — complete (merged to main as PR #4, commit a9376a5, 2026-07-12).**

Done:
- GCP project `openaq-pipeline` (billing linked; free trial started ~2026-07-12, $300/90 days — always-free tier persists after). Region **us-central1**: US regions qualify for the GCS always-free tier and latency is irrelevant for a batch pipeline.
- `infra/` Terraform, 11 resources: API enablement (storage/bigquery/iam, `disable_on_destroy = false`), raw bucket `openaq-pipeline-openaq-raw` (versioned per G1 immutable-raw, uniform bucket-level access, public access prevention enforced, `force_destroy = false`), datasets `openaq_raw` + `openaq_dbt` in us-central1 (colocated with the bucket so GCS→BQ load jobs need no cross-region copy), SA `openaq-pipeline@…` with least-privilege grants — `storage.objectAdmin` on the bucket only, `bigquery.dataEditor` on the two datasets only, `bigquery.jobUser` at project level (BQ jobs are project-scoped; can't be narrower).
- Remote tfstate in `gs://openaq-pipeline-tfstate` (versioned), bootstrapped manually via gcloud — the backend bucket cannot be provisioned by the state it stores. Documented in `infra/README.md`.
- **No tables in Terraform** — `raw_measurements` is still `[ASSERTED]` and dbt owns its own relations; IaC pinning a guessed schema would couple infrastructure to it.
- SA key at `~/gcp-keys/openaq-pipeline-key.json`, created via `gcloud iam service-accounts keys create`, **not** a `google_service_account_key` resource (TF-managed keys store the private key in plaintext in tfstate — G10). Local `.env` GCP values now real; docker-compose key wiring still deferred to Phase 2/3.
- `.terraform.lock.hcl` un-gitignored and committed (pins provider versions; ignoring it was an anti-pattern in the Phase 0 `.gitignore`).
- CI: fourth job `terraform` (`fmt -check` → `init -backend=false` → `validate`; needs no GCP credentials). Added to the branch-protection required checks — all four (`lint`/`dbt-parse`/`pytest`/`terraform`) confirmed required via the GitHub rules API on 2026-07-12.

**Phase 1 exit criteria — verified:**
- [x] Second `terraform apply` → "No changes. Your infrastructure matches the configuration."
- [x] SA smoke test using its own key in an isolated `CLOUDSDK_CONFIG`: wrote an object to the raw bucket, created `openaq_raw.smoke_test` via `bq mk`, then cleaned both up (verified empty after).
- [x] tfstate object present in the GCS backend (`terraform/state/default.tfstate`).

**Phase 2 — complete (`feat/phase-2-ingestion`, 2026-07-13).**

Done:
- `ingestion/openaq/` package: `config.py` (env-driven settings), `client.py` (X-API-Key session; throttles off `x-ratelimit-*` headers; 429 waits for the window, 5xx/connection errors back off exponentially with bounded retries; 401/403 fail fast; pagination terminates on a short page because `meta.found` can be a string like `">1000"`), `ingest.py` (the G2 fan-out, one parameterized entry point per G12), `gcs.py` (raw-zone writer, bucket handle injected for testability), `__main__.py` (manual CLI; Phase 3's DAG imports the functions instead).
- Sensor scope decision: fetch only **pm25/pm10/no2** sensors — the pollutants the marts compare. Filtering is on sensor *metadata* (which sensors to call), not fetched data, so G7 holds. `TARGET_PARAMETERS` is one frozenset; widening it is a one-line change.
- Raw layout implemented + verified (see §5), including the `locations.json` inventory objects (new — see §7.5 for why).
- Per-sensor fault isolation (added after the first live PK run hit a persistently-500ing sensor — see §7.5): failed sensors land in `RunSummary.sensors_failed`, the rest of the fan-out completes, CLI exits 1 on partial failure.
- 22 unit tests (`test_client.py` / `test_ingest.py` / `test_gcs.py`), API fully mocked via `responses` (G12), no network or credentials in CI. Runtime deps (`requests`, `google-cloud-storage`) added to `pyproject.toml` with pins matching `airflow/requirements.txt`; CI pytest job installs `-e .`.
- Live manual runs for 2026-07-12 (UTC): **AE** — 31 locations, 52 target sensors, 8 with data / 44 empty, 192 measurements, 9 objects, exit 0. **PK** — 441 locations, 447 target sensors, 160 with data / 257 empty / 30 failed (all persistent server-side 500s), 3,460 measurements, 161 objects, exit 1 (correct: partial failure is reported, landed data stays). Spot-checked one landed object: byte-for-byte the API page body (24 hourly pm25 records).

**Phase 2 exit criteria — verified:**
- [x] Unit tests green (20 passed; ruff clean; all pinned to the 3.12 venv)
- [x] Manual run lands real raw JSON for 1 day / 1 country (AE and PK both landed; objects listed + spot-checked in GCS)
- [x] Empty responses don't crash (44 of AE's 52 sensors were empty on the target day — skipped and counted, exit 0; also unit-tested)

**Phase 3 — complete (`feat/phase-3-orchestration`, 2026-07-15).**

Done:
- `airflow/dags/openaq_ingest.py`: daily 02:00 UTC for the previous complete UTC day (`ds`). Per country (static task group; nested mapping isn't supported): `prepare_country_run` (lands `locations.json`, emits the sensor list) → `fetch_sensor` **dynamically mapped** over the sensors (G3) behind the `openaq_api` pool (4 slots vs the 60 req/min limit; client `max_attempts=2`) → `summarize_country`. Then `ensure_raw_table` (DDL `IF NOT EXISTS`; Terraform still owns no tables) → `load_raw_to_bq` → `reconcile_counts`.
- **Failure model — catch + threshold (decided 2026-07-15):** mapped fetch tasks return `status="failed"` as *data* instead of raising; a country fails only above 20% sensor-fetch failures. Rationale: ~30 persistently-broken PK sensors make per-sensor red tasks a permanent false alarm — "failed DAG" must stay a real alert. Auth errors still fail the run.
- **Load design:** `BigQueryInsertJobOperator` over a temp external table (CSV, `\x01` delimiter, quoting off → one string column per NDJSON line): `INSERT … SELECT PARSE_JSON(line, wide_number_mode=>'round'), CURRENT_TIMESTAMP(), _FILE_NAME`. `_FILE_NAME` → `source_uri` carries identity (G1/G4); append-only, dbt dedups. Emits Dataset `bigquery://openaq-pipeline/openaq_raw/raw_measurements` (G9 — Phase 4's schedule contract).
- `reconcile_counts`: latest-batch measurement total in BQ (excluding `locations.json` rows) must equal the run's API-side count; `ingested_at` is one `CURRENT_TIMESTAMP()` per INSERT, so `MAX(ingested_at)` isolates a batch across reruns.
- Ingestion refactor: `fetch_sensor_day()` extracted as the mapped-task unit of work (XCom-safe dict contract, unit-tested); `OpenAQClient(max_attempts=…)`.
- Wiring: `./ingestion` + SA key mounted into the containers (closes the Phase 0-deferred keys gap); in-container `GOOGLE_APPLICATION_CREDENTIALS` is the fixed container path (the `.env` value is the *host* path for CLI runs); `google_cloud_default` connection via JSON env var (G10); `openaq_api` pool created idempotently by `airflow-init` (which runs as root — see §7.5).
- Tests/CI: `tests/dags/test_dag_integrity.py` (DagBag import, structure, mapped fetch + pool, Dataset outlet, source URIs) in a new **`dag-validate`** CI job that mirrors the image's constrained install; `make dag-test` for the quick in-container check. 26 unit tests (4 new).
- Dependency reproducibility fix: image + CI installs now apply the official constraints-2.9.1 file; pins aligned (google provider 10.17.0, google-cloud-bigquery 3.21.0, numpy 1.26.4) — see §7.5.

**Phase 3 exit criteria — verified (live, 2026-07-15, ds=2026-07-14):**
- [x] DAG green end-to-end: run succeeded — AE 8/52 sensors with data (192 measurements), PK 180/447 (3,624), 35 failed PK sensors = 7.8% < threshold, run stays green by design
- [x] BQ counts reconcile vs API: 3,816 == 3,816 (reconcile task, latest batch)
- [x] Same-day rerun safe: full rerun green; two `ingested_at` batches with identical shape (190 page rows / 4,288 records / 2 locations objects each) — append confirmed, dedup deferred to dbt (G4)
- [x] Dataset emitted: `dataset_event` rows recorded on load success (visible in the UI Datasets tab)

**Deferred to Phase 5 (recorded, deliberate):** G4 rolling lookback (re-fetch last N days) and known-bad-sensor tracking — both belong with the wide-window backfill mechanics. Scheduled runs only fire while the compose stack is up; missed days are backfill's job (`catchup=False`).

**Phase 4 — complete (`feat/phase-4-transformation`, 2026-07-17; all four exit criteria verified live).**

Done:
- `astronomer-cosmos==1.15.0` added to `airflow/requirements.txt` — verified to resolve cleanly under constraints-2.9.1 with all existing pins (`pip check`, 256 packages).
- `dbt/seeds/who_thresholds.csv` (G5) with explicit `averaging_period` values — the O3 labeling debt (§7.5) paid: `8h`/`peak_season`, not the constants.py shorthand. `tests/unit/test_who_seed_sync.py` enforces the seed↔`constants.py` sync that was previously a comment-only contract.
- Models per §5: three staging views (G1 parse + G4 dedup on `(sensor_id, period_start_utc)` by latest `ingested_at`), `int_daily_aqi` (G7 completeness columns), `mart_country_compare` (G6 24h-only join on pollutant **and unit**; G8 explicit denominators; a station-day with no matching threshold keeps a null flag and drops out of the denominator, never out of the table), `mart_annual_compare` (G6 annual grain). 51 dbt data tests across layers.
- `airflow/dags/openaq_transform.py`: cosmos `DbtDag` scheduled on the raw_measurements Dataset (G9). The Dataset moved to a shared non-DAG module `openaq_datasets.py` imported by both DAGs — see §7.5 for why a DAG file must not import another DAG file.
- Local `dbt build` against live BigQuery: **58/58 green** (1 seed, 6 models, 51 tests). G4 dedup verified against the duplicated 2026-07-14 double batch: `stg_measurements` = 3,816 = exactly half the raw records and equal to Phase 3's reconciled count.
- Hand-verified exceedance flags by recomputing station-day means straight from raw JSON: location 6135452 (PK pm25 avg 120.9 µg/m³ → exceeded) and boundary case 6135285 (avg exactly 15.0 → **not** exceeded; the guideline is "should not exceed", so strict `>` is correct).
- Tests/CI: `tests/dags/test_dag_transform.py` (14 cosmos tasks, Dataset-trigger == ingest outlet, tests gate downstream models) + shared `tests/dags/conftest.py`; SQLFluff now **blocking** in CI (deliberate Phase 4 decision per §7.5, `.sqlfluff` config committed, ST06 excluded deliberately); 27 unit tests + 9 DAG tests green.
- First analytical signal (single day, 2026-07-14, pm25 only — the only parameter with data that day): AE 8/8 reporting stations exceeded the WHO 24h guideline (country mean 69.7 µg/m³), PK 169/180 (93.9%, mean 35.3 µg/m³).

**Phase 4 exit criteria — verified (live, 2026-07-17):**
- [x] dbt run + test green — locally against live BQ (58/58) and again via the DAG (all cosmos tasks green)
- [x] 1 exceedance flag hand-verified (two, including a threshold-boundary case)
- [x] Cosmos per-model tasks — 14 tasks (run+test per node), asserted by DAG tests and observed live
- [x] transform DAG triggers off the Dataset — the scheduled ingest run (ds=2026-07-16) emitted the Dataset event and `dataset_triggered__2026-07-17T08:51:53` ran to success (14/14 tasks, 66s); marts absorbed the new day (4 rows, rolling_7d_avg hand-checked). The trigger was delayed ~21 min by a root-owned scheduler-log dir killing DAG serialization — see §7.5; the queued event survived and fired the instant parsing recovered, which is itself a nice property of the Dataset queue.

**Not started:** §6 Phases 5–7. No backfill, Elementary, or dashboard yet.

**Known liabilities carried forward:** the "remove CI workflows" commit remains in history (6524216) — not rewritten, just superseded.

**Pre-Phase-1 hygiene pass (2026-07-07 audit).** A repo audit found that Phase 0's scaffold cleanup removed anti-pattern *code* but not the docs describing it, and that "no empty scaffold files" was not fully true. Fixed in the `chore/pre-phase-1-hygiene` PR:
- Rewrote six stale scaffold-era READMEs (root, `ingestion/`, `dbt/`, `airflow/`, `tests/`, `scripts/`) that still described the pre-correction design as present "Contents" — including per-country fetchers (`uae.py`/`pakistan.py`, banned by G12), a `bootstrap.sh` that "provisions GCP resources" (contradicts G11; the real script only checks tools and copies `.env`), and instructions to copy a gitignored `profiles.yml` (opposite of the implemented G10 decision). READMEs now describe what exists and mark future files as "planned (Phase N)".
- Deleted four remaining empty tracked files: `ingestion/openaq/fetchers/__init__.py` (skeleton of the removed anti-pattern), `dbt/profiles.yml.example` (obsolete under G10), `airflow/config/airflow.cfg` (never mounted by docker-compose), `tests/conftest.py` (redundant — pytest `pythonpath` config covers imports).
- Fixed invalid `build-backend` in `pyproject.toml` (`setuptools.backends.legacy:build` → `setuptools.build_meta`); the bad value would have broken any future `pip install -e .`.
- Verified externally: CI runs green on main, branch-protection ruleset active with the three required checks (via GitHub API); `openaq_architecture_spec.md` exists nowhere in the tree or git history; `.env` was never tracked.

**Pre-Phase-2 audit (2026-07-12, `chore/pre-phase-2-hygiene`).** A full audit (repo, GitHub state, live GCP, docs) before starting Phase 2:
- Verified good: all 4 CI jobs green on PR #4 and main; branch-protection ruleset requires all four checks; live GCP matches Terraform (tfstate object present, bucket + both datasets exist in us-central1); commit history clean (conventional commits, no stray trailers); no guardrail regressions; no secrets tracked.
- Fixed two latent dbt config bugs that would have failed in Phase 4: `profiles.yml` had `location: US` while the datasets live in **us-central1** (BQ jobs must run in the dataset's location), and `dbt_project.yml` set `+schema: dbt`, which dbt's default schema-name generation appends to the profile dataset — models would have targeted a nonexistent `openaq_dbt_dbt` dataset the least-privilege SA cannot create.
- Fixed stale root README (still claimed "Phases 1–7 not started" after Phase 1 merged) and `docs/README.md` (promised a data dictionary and runbooks that don't exist; omitted PROJECT_CONTEXT.md from its own contents).
- Resolved the Python drift (see §7.5): `.venv` rebuilt on CPython 3.12.13 via a userland `uv` install; ruff + pytest green on 3.12.
- Re-probed the OpenAQ API key: **still 401** (see §8) — must be regenerated before Phase 2 work starts.

**Pre-Phase-3 audit (2026-07-14, `chore/pre-phase-3-hygiene`).** Audit before starting Phase 3:
- Verified good: all 4 CI checks green on main (`lint`/`dbt-parse`/`pytest`/`terraform`); branch-protection ruleset enforcing all four required checks plus PR, force-push, and deletion rules (via GitHub rules API); no credential patterns in any tracked file; live GCP matches Terraform (tfstate object present, raw bucket + both datasets in place); root README phase status current.
- Docs restructure: scoped this file to architecture, guardrails, and phase state — session-specific working notes and project framing details moved out of the versioned doc; §0 trimmed to doc-maintenance rules. `docs/architecture.md` "Why this stack" wording aligned with §1.

**Pre-Phase-4 audit (2026-07-16, `chore/pre-phase-4-hygiene`).** Full audit (code, git history, docs) before starting Phase 4:
- Verified good: no secrets in any commit or tracked file (`.env`/key files never entered git; the one base64-looking string in history is a Terraform provider checksum in `.terraform.lock.hcl`); all 31 tests match current code; ingestion client/DAG logic clean against G1–G12; dbt configs still correct for Phase 4 (`profiles.yml` us-central1 + env_var-only, no `+schema` overrides) and the least-privilege SA needs no new grants for dbt; root/`airflow`/`ingestion`/`dbt`/`docs` READMEs accurate.
- Fixed: stale `tests/README.md`; §10 changelog row order; §2 CI cell (five jobs); `bootstrap.sh` fill-in list (`GCP_KEY_FILE`, `FERNET_KEY`); two stale `infra/README.md` lines; empty-expansion guard in `prepare_country_run` (see §7.5).
- Phase 4 note: `astronomer-cosmos` is in the planned stack but in no dependency file yet — adding it (with a pin that resolves under the Airflow 2.9.1 constraint set, compatible with dbt 1.8) is the first Phase 4 work item. `db-dtypes==1.2.0` in `airflow/requirements.txt` is currently unused (no pandas path in the DAG) — left in place; revisit at Phase 4 closeout when dbt runs in the image.

**Pre-Phase-5 audit (2026-07-18, `chore/pre-phase-5-hygiene`).** Three-track audit (ingestion/DAG code, dbt layer, repo/docs) before starting Phase 5:
- Verified good: guardrails G1–G12 hold everywhere; seed values match WHO 2021 exactly and the constants↔seed sync test is exact bidirectional equality; strict-`>` exceedance, pollutant+unit threshold joins, G4 dedup key, and G6 grain separation all correct; pins agree across CI/pyproject/requirements (constraints-2.9.1 applied, `pip check` guarded); nothing mis-tracked, no co-author trailers, conventional commits; root README status/layout/quickstart accurate. (`gh` not installed locally — GitHub branch-protection state not re-probed; the hygiene PR's five green checks re-verify it.)
- Fixed (code): `rolling_7d_avg` computed over 7 *rows*, not 7 calendar days — day gaps silently widened the window; now a `range` frame over `unix_date(measurement_date)`. Dockerfile base pinned to `apache/airflow:2.9.1-python3.12` (the dag-validate job hardcodes 3.12 constraints — the mirror guarantee was accidental with an unpinned variant). Client no longer sleeps after the final failed retry attempt (pure wasted latency on every known-bad sensor). Dropped confirmed-unused `db-dtypes` (the §7.5 "revisit at Phase 4 closeout" item) and `sqlfluff-templater-dbt` (jinja templater is the deliberate choice).
- Fixed (tests): `stg_measurements.unit` was an untested threshold-join key — a unit drift would silently null flags and shrink the G8 denominator with no test firing; now `not_null` + `accepted_values`. Behavioral tests added for the DAG failure model (20% threshold incl. the exactly-at-threshold boundary, zero-sensor guard, mapped-task spec shape) and the client's 429 fallback wait — these branches previously had only structural coverage. dbt: 55 tests / 62 nodes; pytest: 28 unit + 13 DAG.
- Fixed (docs): G8 amended to record the realized per-day stations rate (decision: days-based rate deferred to Phase 5/6 with backfill); §10 v1.8/v1.9 row order; stale pre-Phase-4 tense in `ingestion/README.md` and `constants.py`; mart header comment named the wrong denominator column.

## 7.5 Deviations and discoveries (for institutional memory)

- **Local Python version drift — RESOLVED 2026-07-12.** Host `python3` resolves to 3.14.4, while `pyproject.toml`'s `requires-python`, the Airflow Docker image, and this document's §2 stack table all target 3.12. Resolved in the pre-Phase-2 hygiene pass: `uv` installed userland (no sudo needed; `~/.local/bin/uv`), `.venv` rebuilt on a uv-managed standalone CPython **3.12.13**, dev deps reinstalled, lint + tests green. Local dev now matches CI and the Airflow image.
- **`sqlfluff` is currently a no-op.** `continue-on-error: true` on the SQLFluff CI step was added defensively, but empirically (tested locally) SQLFluff exits 0 on an empty `dbt/models/` directory regardless. The flag has zero effect today. It becomes load-bearing in Phase 4 when real `.sql` files land — at that point, decide explicitly whether lint failures should block merges (remove the flag) or only warn (keep it, but make that a deliberate choice, not inherited inertia).
- **GitHub Rulesets, not Classic branch protection.** Used the newer Rulesets UI instead of Classic. Functionally equivalent for our needs (require PR, require status checks, block force push) but the setup flow is non-obvious — a new ruleset defaults to Enforcement: Disabled and no target branch, both of which must be explicitly set or the rule silently does nothing while looking configured.
- **Required-check names matter for branch protection.** Two CI jobs were initially both named `test` (intended to simplify required-checks down to one name); this was a real bug, not a style choice — GitHub's Checks API keys on the job's `name:` field, and whichever job reports last silently overwrites the other's status, making branch protection non-deterministic. Caught before merge; fixed to `lint`/`dbt-parse`/`pytest`.
- **(2026-07-07 audit) Scaffold READMEs are part of the design surface.** The Phase 0 cleanup deleted anti-pattern code but left six READMEs describing that code as present — a future session scaffolding from them would have rebuilt the banned design. Lesson: when a guardrail kills a pattern, grep the *docs* for it too.
- **(2026-07-07 audit) `docker-compose.yml` defaults `GOOGLE_APPLICATION_CREDENTIALS` to `/opt/airflow/keys/service-account.json`, but no `./keys` volume is mounted.** Deliberately deferred to Phase 2/3 when GCP auth becomes real — decide then between mounting a keys dir or another delivery mechanism.
- **(2026-07-07 audit) O3 threshold labeling debt in `ingestion/constants.py`.** The dict stores O3's 8-hour value under the `"24h"` key and peak-season under `"annual"` (commented, tested). Acceptable shorthand for a two-key dict; the Phase 4 `who_thresholds` seed has an explicit `averaging_period` column and must record `8h` / `peak_season` correctly, not inherit the shorthand.
- **(Phase 1) No sudo in the WSL session** → CLIs installed userland: terraform as a single binary in `~/.local/bin`, Google Cloud SDK via tarball in `~/google-cloud-sdk` with `gcloud`/`gsutil`/`bq` symlinked into `~/.local/bin` (no `.bashrc` edits). `unzip` was also missing (used Python's `zipfile`). If sudo becomes available, apt-based installs would give managed updates.
- **(Phase 1) Two separate gcloud logins.** `gcloud auth login` (CLI identity) and `gcloud auth application-default login` (ADC) are distinct; Terraform authenticates via **ADC** only. Forgetting the second yields provider auth errors despite a "logged in" gcloud.
- **(Phase 1) Backend blocks cannot interpolate variables** — the tfstate bucket name is a literal in `main.tf`, not `var.project_id`. Known Terraform limitation; acceptable for a single-env project (multi-env would use partial backend config via `-backend-config`).
- **(Phase 1) The GCP project pre-existed.** Planning assumed a from-scratch account, but `openaq-pipeline` (billing linked) already existed alongside unrelated projects — worth checking `gcloud projects list` before scripting account setup steps.
- **(2026-07-12 audit) dbt job location must equal the dataset's region, exactly.** `location: US` (multi-region) in `profiles.yml` is not a superset that covers `us-central1` datasets — BigQuery jobs run in one location and fail with "dataset not found" on mismatch. Any future region change in Terraform must be mirrored in `profiles.yml`.
- **(Phase 2) Measurement payloads carry no sensor/location ids.** `/v3/sensors/{id}/measurements` records contain value/parameter/period/coverage but no identifier tying them back to the sensor or location (verified live 2026-07-13). Identity therefore rides on the GCS object path (`{sensor_id}.json`), which Phase 3 must preserve into `source_uri`; and each run also lands the verbatim `/v3/locations` pages as `locations.json` so dbt can join sensors to locations/coordinates/monitor-type. Without that landing, Phase 4 would have to call the API from dbt — a non-starter.
- **(Phase 2) `/v3/locations` embeds each location's sensor list** (id + parameter), so the G2 fan-out needs no per-location sensors call: a country-day costs `2 + n_sensors` requests (+1 per countries page).
- **(Phase 2) The coverage gap is visible in pure metadata.** AE: 31 locations, 18 reference monitors, sensors spread across pollutants (pm10 21 / no2 17 / pm25 14). PK: 441 locations but only 5 reference monitors, 441 pm25 low-cost sensors, 6 pm10, and **zero no2 sensors** — the NO2 comparison is empirically one-sided with current OpenAQ coverage; the marts must surface this, not paper over it. Also AE's instrumentation is partly dormant: only 8/52 target sensors reported data on 2026-07-12.
- **(Phase 2) Individual sensors can be persistently broken server-side.** The first live PK run aborted at sensor 15904590, which returns an instant HTTP 500 on every attempt (verified by direct probe — no rate-limit headers, not a throttling artifact). Retries cannot fix a server-side data bug, and one broken sensor out of 447 must not lose the rest of the fan-out. Fixed: per-sensor failures are isolated into `RunSummary.sensors_failed` and the CLI exits 1 on partial failure — landed data stays, but partial success is never silent. Auth errors (401/403) remain fatal for the whole run. The full PK run then found **30** such sensors — in contiguous id blocks (16034750–79, 16242897–915), likely newly-onboarded batches with a broken data backend — so Phase 3's DAG must treat nonzero `sensors_failed` as normal, not a reason to discard the run. Cost note: 5 retry attempts × exponential backoff ≈ 62s per broken sensor (~31 min of the 39-min PK run); Phase 3/5 should cut attempts for instant 5xxs or track known-bad sensors.
- **(Phase 2) `meta.found` is not always an int** (the API can report `">1000"`), so pagination terminates on `len(results) < limit`, never on `found`.
- **(Phase 3) Unconstrained pip on top of the Airflow image is a time bomb.** The Dockerfile installed `airflow/requirements.txt` without the official constraints file; pip pulled numpy 2.x against the base image's numpy-1.x-ABI pandas, so every BigQuery-operator import would have crashed at runtime. Caught by running the new DagBag tests in a clean constrained venv *before* any container ran. Rule: anything that installs on top of `apache/airflow:X` (Dockerfile, CI) applies `constraints-X` and keeps explicit pins agreeing with it (google provider 10.17.0, google-cloud-bigquery 3.21.0, numpy 1.26.4).
- **(Phase 3) Airflow `template_ext` treats *any* templated string ending in `.json`/`.sql` as a template FILE to load.** The load's `sourceUris` ending in `/*.json` failed at render time with `TemplateNotFound` on the URI itself. Fix: wildcard ends at `/*` (equivalent for our layout, and it usefully loads `locations.json` too — see §5). Applies to every string inside an operator's `template_fields`, not just ones containing `{{ }}`.
- **(Phase 3) Dynamic mapping can only expand over a task's default `return_value` XCom** — `.expand()` over a `multiple_outputs` key fails at parse time. `prepare_country_run` returns the bare sensor-spec list. Also: the google provider validates `bigquery://` Dataset URIs as full `project/dataset/table`.
- **(Phase 3) BQ job location, part two.** The pre-Phase-2 lesson (jobs run in exactly one location = the dataset's region) resurfaced twice: `BigQueryHook.get_records()` demands an explicit `location`, and even with it the hook's DB-API layer created the job in one location and polled another (404). `reconcile_counts` uses the hook's native client with `query_and_wait` and `BIGQUERY_LOCATION` (env, default `us-central1`) instead.
- **(Phase 4) The root `airflow-init` poisons the scheduler-log date dir — and dead processors silently kill Dataset scheduling.** Sequel to the bullet below: init's CLI calls initialize file-processor logging and create `logs/scheduler/<today>/` root-owned (755) before the scheduler starts. Every DAG-file processor child then dies at bootstrap with `PermissionError`, so DAGs are never (re)serialized — the live symptom was a Dataset event sitting queued in `dataset_dag_run_queue` for 21 minutes with no run created, while the ingest DAG (already serialized) ran normally. Fixed in compose: init chowns `/opt/airflow/logs` back to `AIRFLOW_UID` as its last step. Debug path worth remembering: `dataset_event` (event recorded?) → `dataset_dag_run_queue` (queued for the consumer?) → `serialized_dag.last_updated` (is the consumer's serialization fresh?). The queued event fired the moment parsing recovered.
- **(Phase 3) `airflow-init` must run as root when AIRFLOW_UID is an arbitrary host uid.** The init service overrides the image entrypoint with plain bash, bypassing the arbitrary-uid passwd handling, so the CLI dies with `getpwuid(): uid not found`. Upstream's reference compose runs init as `0:0` for the same reason; long-running services stay `${AIRFLOW_UID}:0`. Related: the `logs/` bind mount needed a one-time chown (owned by uid 50000 from a pre-Phase-3 stack-up under the old default).
- **(Phase 3) The PK broken-sensor population grew: 35 failed sensors on ds=2026-07-14** (superset pattern of the 30 seen in Phase 2, still contiguous id blocks). Validates the catch+threshold failure model — 7.8% < 20% keeps the run green while the summary logs every failed id. Known-bad tracking still Phase 5.
- **(2026-07-16 audit) An empty dynamic-mapping expansion is skipped, not failed.** `.expand()` over an empty list marks the mapped task (and, by default trigger rules, its downstream) skipped — a country resolving to zero target sensors would silently skip toward the load path instead of erroring. Latent only (AE and PK always have pm25/pm10/no2 sensors), but `prepare_country_run` now raises on an empty sensor list: a zero-sensor country-day is anomalous and must be loud, especially before any new country is onboarded.
- **(Phase 4) A DAG file must never import another DAG file.** The transform DAG first imported its Dataset from `openaq_ingest`; executing that module inside the transform file's parse auto-registered the ingest DAG under *both* files (`AirflowDagDuplicatedIdException` in the DagBag tests; in production, two files fighting over one dag_id). Shared contracts (the Dataset URI) live in a non-DAG module (`openaq_datasets.py`). Related: production Airflow puts DAGS_FOLDER on `sys.path` (module-management docs), which is what makes such sibling imports work at all — standalone DagBag tests must replicate it (done in `tests/dags/conftest.py`).
- **(Phase 4) The cosmos dbt-ls cache needs the Airflow metadata DB at parse time.** Cosmos persists its render cache to an Airflow Variable; in DB-less environments (DagBag tests, the dag-validate CI job) DAG import dies with `no such table: variable`. Disabled there via `AIRFLOW__COSMOS__ENABLE_CACHE=false` (set in `tests/dags/conftest.py`); the real deployment keeps the cache.
- **(Phase 4) SQLFluff decision (closes the Phase 0 note above): blocking.** `continue-on-error` removed from the CI lint step — SQL style failures now block merges like ruff. `.sqlfluff` uses the jinja templater with dbt builtins (the dbt templater would need credentials in CI) and deliberately excludes ST06 (column order: it would force audit columns above entity ids). RF04 caught `value` as a keyword identifier → column is `measurement_value`.
- **(2026-07-18 audit) A `rows` window frame is only a time window when the series is gapless.** `rolling_7d_avg` used `rows between 6 preceding and current row`, which on a gappy daily series (guaranteed here — G7 keeps thin coverage visible instead of filtering it) spans however many *calendar* days the last 7 rows happen to cover. BigQuery `range` frames need a numeric ORDER BY, so the fix is `order by unix_date(measurement_date) range between 6 preceding and current row`. Rule: any rolling-window column named in time units must use a range/interval frame or a date spine, never a row count.
- **(2026-07-12 audit) dbt `+schema:` is a suffix, not a target.** With the default `generate_schema_name` macro, `+schema: dbt` on top of a profile `dataset: openaq_dbt` yields `openaq_dbt_dbt`. The least-privilege SA (dataset-scoped `dataEditor`, no dataset-create permission) would have turned this into a hard permission failure in Phase 4 — removed the overrides; the profile's dataset is the single source of the target.

---

## 8. Genuinely open questions

- **OpenAQ v3 free-tier rate limits — partially answered 2026-07-12:** response headers on a live call show `x-ratelimit-limit: 60` with `x-ratelimit-reset: 60`, i.e. **60 requests/minute**. The per-sensor fan-out makes this load-bearing: the Phase 2 client must throttle/backoff off these headers, and backfill (Phase 5) must budget for it. Whether an additional daily cap exists is still unconfirmed — watch for it during the first real ingestion runs.
- ~~**The `OPENAQ_API_KEY` in `.env` is invalid**~~ **RESOLVED 2026-07-12:** the old key 401'd on probes of `/v3/countries` (2026-07-07 and 2026-07-12); regenerated at explore.openaq.org and verified live — HTTP 200, rate-limit headers captured (see above). No longer a Phase 2 blocker. (The 2026-07-07 probe also confirmed G2 empirically: `/v3/measurements?countries_id=...` returns 404 — the flat endpoint does not exist.)
- **Backfill volume — now quantified (2026-07-13 recon):** a country-day costs `2 + n_sensors` requests. AE = 52 target sensors (~1 min/day at 60 req/min), PK = 447 (~7.5 min/day). Day-by-day backfill of PK × 365 days ≈ 165k requests ≈ **45 hours** — not viable. Phase 5 must widen the `datetime_from/to` window per sensor (the endpoint paginates at 1000 records; an hourly sensor fits ~41 days/page), cutting PK×1yr to roughly 4k requests ≈ 1.5 h. No additional daily cap was observed across ~500 requests in the Phase 2 runs — still watch during backfill.
- **Verified vs asserted schema** — the GCS raw layout and measurement payload shape are now **verified** (§5, §7.5). `raw_measurements` (BQ table) and the parsed staging columns remain `[ASSERTED]` until Phase 3 lands the load job.
- **Is `datetime_to` inclusive? (flagged 2026-07-18)** The ingest window per sensor-day is `[dayT00:00:00Z, (day+1)T00:00:00Z]`. If the API treats `datetime_to` as inclusive, the next day's 00:00 reading lands in *two* day-partitions. Benign today (staging dedups on `(sensor_id, period_start_utc)` across batches), but Phase 5's wide, overlapping backfill windows make the boundary semantics worth confirming empirically before choosing window edges.

---

## 9. Source-of-truth facts (verified)

- WHO thresholds in §4/G5 are the **2021** Global Air Quality Guidelines (verified 2026-06-18).
- OpenAQ **v3** is sensor-centric per §4/G2 (verified against OpenAQ docs 2026-06-18): `countries → locations → sensors → measurements`, country filtered by `countries_id`.
- `countries_id`: **AE = 59, PK = 109** (verified live 2026-07-13).
- Measurements endpoint takes `datetime_from`/`datetime_to` (ISO-8601 Z) + `limit`/`page`; records are period-aggregated (`period.label: "raw"`, hourly interval observed) with a `coverage` block — verified live 2026-07-13.

---

## 10. Changelog

| Version | Date | Change |
|---|---|---|
| 1.0 | 2026-06-18 | Initial context doc. Captures architectural review corrections (G1–G12), phase roadmap, and project framing. |
| 1.1 | 2026-06-30 | Phase 0 complete (merged main as 700fe1a). CI green, branch protection active, all scaffolds resolved, Fernet key rotated. Added §7.5 documenting Python version drift (host 3.14 vs target 3.12), sqlfluff no-op status until Phase 4, and the duplicate-job-name branch-protection bug caught pre-merge. |
| 1.2 | 2026-07-07 | Pre-Phase-1 audit + hygiene PR. Rewrote six stale scaffold-era READMEs that still described the banned pre-correction design; deleted four surviving empty tracked files; fixed invalid pyproject build-backend. Recorded new open question (OPENAQ_API_KEY 401) and §7.5 discoveries (docs are design surface; docker-compose keys-mount gap; O3 seed labeling debt). G2 empirically confirmed: flat /v3/measurements returns 404. |
| 1.3 | 2026-07-12 | Phase 1 complete (`feat/phase-1-terraform`). All GCP via Terraform (G11): raw bucket, two datasets, least-privilege SA, remote tfstate in GCS; all three exit criteria verified (idempotent apply, SA smoke test, remote state). Lock file now committed; `terraform` CI job added. §7.5 additions: userland CLI installs (no sudo), ADC vs CLI auth split, backend-block variable limitation, pre-existing GCP project. |
| 1.4 | 2026-07-12 | Pre-Phase-2 audit + hygiene PR (Phase 1 merged as PR #4). Fixed two latent dbt config bugs (profiles `location: US` vs us-central1 datasets; `+schema: dbt` → `openaq_dbt_dbt` doubling); refreshed stale root and docs READMEs; recorded `terraform` as a verified required check. Python drift resolved: `.venv` rebuilt on uv-managed CPython 3.12.13. OpenAQ key re-probed: still 401 — hard Phase 2 blocker until regenerated. |
| 1.5 | 2026-07-13 | Phase 2 complete (`feat/phase-2-ingestion`): tested v3 client (`ingestion/openaq/`), G2 fan-out to GCS raw NDJSON, 22 mocked unit tests, live AE+PK runs verified all three exit criteria. Per-sensor fault isolation added after a persistently-500ing PK sensor (15904590) aborted the first live run. §5 raw layout verified + extended with `locations.json` (measurement payloads carry no ids). §7.5: metadata-visible coverage gap (PK has zero no2 sensors; AE instrumentation partly dormant); `meta.found` can be a string. §8: backfill budget quantified — day-by-day PK backfill is 45h, Phase 5 must use wide datetime windows. §9: countries_id AE=59/PK=109. |
| 1.6 | 2026-07-14 | Pre-Phase-3 audit + hygiene PR. Verified CI, branch protection, secrets hygiene, and live-GCP-vs-Terraform alignment ahead of Phase 3. Scoped this doc to architecture and state: split session-specific working notes out of the versioned doc, trimmed §0 to maintenance rules, removed the former §9 (sections renumbered), and aligned `docs/architecture.md` wording. |
| 1.7 | 2026-07-15 | Phase 3 complete (`feat/phase-3-orchestration`): `openaq_ingest` DAG — dynamic mapping over sensors behind an API pool (G3), catch+threshold failure model (20%), external-table load into `raw_measurements` with `_FILE_NAME`→`source_uri` (G1/G4), Dataset emitted (G9), reconcile task. All four exit criteria verified live (ds=2026-07-14: 3,816 measurements reconciled; rerun appended an identical second batch). `raw_measurements` schema flips to VERIFIED; `locations.json` pages land in the raw table by design. New `dag-validate` CI job; image/CI installs now constraint-pinned after a live numpy-ABI break. §7.5: template_ext footgun, expand-over-keyed-XCom, BQ job-location part two, airflow-init as root, PK broken sensors now 35. |
| 1.8 | 2026-07-16 | Pre-Phase-4 audit + hygiene PR. Full sweep (code, git history, docs): no secrets anywhere in history or tree, all 31 tests current, guardrails hold, dbt/Terraform configs re-verified ready for Phase 4. Fixed: stale `tests/README.md` (still described the Phase 0 tree), §10 changelog row order (v1.6/v1.7 were swapped), §2 CI cell (now lists all five jobs), `bootstrap.sh` env-var list (`GCP_KEY_FILE` is hard-required by compose; `FERNET_KEY` generation hint), two stale `infra/README.md` lines, ignore rules hardened. New: empty-expansion guard in `prepare_country_run` — a country resolving to zero target sensors now fails loudly instead of skip-cascading toward the load. |
| 1.9 | 2026-07-17 | Phase 4 complete (`feat/phase-4-transformation`): who_thresholds seed (O3 labeling debt paid, sync test), 3 staging views + int_daily_aqi + two marts (G1/G4/G6/G7/G8) with 51 dbt tests, cosmos 1.15.0 `DbtDag` scheduled on the raw_measurements Dataset via shared `openaq_datasets.py` (G9). All four exit criteria verified live: dbt 58/58 locally and 14/14 cosmos tasks in-DAG; dedup halves the duplicated 2026-07-14 batch exactly; two exceedance flags hand-verified from raw JSON (incl. a 15.0-boundary case); the ds=2026-07-16 ingest's Dataset event auto-triggered the transform run. SQLFluff now blocking. §7.5: DAG files must not import DAG files; cosmos cache needs the metadata DB; root airflow-init poisons the scheduler-log dir and silently blocks Dataset scheduling (compose fix: init chowns logs back). |
| 1.10 | 2026-07-18 | Pre-Phase-5 audit + hygiene PR. Verified clean: guardrails hold across ingestion/DAGs/dbt, seed matches WHO 2021 exactly with real sync-test coverage, pins agree (CI/pyproject/requirements), nothing mis-tracked, READMEs accurate. Fixed: `rolling_7d_avg` was a 7-*row* window (now a 7-calendar-day range frame over `unix_date`); Dockerfile base pinned to `2.9.1-python3.12` (CI's 3.12-constraints mirror was accidental before); client no longer sleeps after its final retry attempt; dropped unused `db-dtypes` + `sqlfluff-templater-dbt`; §10 row order (v1.8/v1.9 swapped — again); stale `ingestion/README.md`/`constants.py` tense. G8 amended to record the realized per-day stations rate; days-based rate deferred to Phase 5/6. New tests: `unit` threshold-join key guarded by accepted_values (silent-denominator-shrink risk), behavioral tests for the 20% failure threshold + zero-sensor guard, 429-fallback wait (dbt 55 tests / 62 nodes; pytest 28 unit + 13 DAG). §8: confirm `datetime_to` boundary inclusivity before overlapping backfill windows. |
