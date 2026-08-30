# Runbook

Day-to-day operation of the pipeline: how to tell it's healthy, how to recover
when it isn't, and what it will never tell you on its own.

Everything here assumes a sourced `.env` and, for the container commands, a
running stack (`make up`).

## Quick reference

| Question | Command |
|---|---|
| Is the data current? | `make freshness` |
| Did the last runs pass their tests? | `make elementary-report` → `elementary_report.html` |
| Do the DAGs still import? | `make dag-test` |
| Does the code still pass? | `make lint && make test` |
| Is the load contract still live end-to-end? | `make integration-test` |

## Is it healthy?

`make freshness` is the single check that answers "is data still arriving?" It
runs dbt source freshness against `raw_measurements.ingested_at` — **warn at 30
hours, error at 54** (thresholds in `dbt/models/staging/sources.yml`). The daily
DAG runs at 02:00 UTC, so 30 hours is one missed run plus slack.

It is deliberately *not* a task inside the transform DAG: that DAG is triggered
by a Dataset event emitted when data has just landed, so asking it whether data
landed recently would always answer yes. A freshness check has to run on a
clock that keeps ticking when the pipeline stops.

## Nothing alerts you when the stack dies

Say this plainly, because the project has already paid for it: **nothing in this
setup notices that it stopped.**

On approximately 2026-07-22 the Compose `scheduler` and `webserver` exited 127
and stayed down. Nobody noticed for five weeks. `catchup=False` means a stack
brought back up creates only the single latest missed run, and the ingest DAG's
7-day lookback heals at most a week — so the outage left a **39-day hole**
(2026-07-22 → 2026-08-29) that no amount of waiting would have filled. It had
to be backfilled by hand once someone looked.

The detector that exists is `make freshness`. Nothing runs it on a schedule,
and nothing routes its result anywhere. On a laptop-hosted stack that is the
honest state: the pipeline is only as observed as its operator. A real
deployment would put freshness on an independent scheduler and alert on the
error threshold — that is the gap, named rather than papered over.

**So: run `make freshness` when you come back to this project.** It is the
cheapest thing that distinguishes "healthy" from "silently dead for a month."

## Recovering from an outage

### 1. Find the hole

```sql
select country_code, max(measurement_date) as last_day
from `<project>.openaq_dbt.mart_country_compare`
group by country_code;
```

Compare each to yesterday (UTC). If the gap is **7 days or less**, do nothing:
bring the stack up and the next scheduled run's rolling lookback covers
`[ds−6, ds+1)`, re-fetches the missing days, and staging's latest-`ingested_at`
dedup absorbs the overlap. This is verified behaviour, not a hope.

If the gap is **longer than 7 days**, the lookback cannot reach it. Backfill.

### 2. Backfill the window

One chunk per country over `[last_good_day, today)` — the window is half-open,
so `--end` is exclusive and today's incomplete UTC day is correctly left out:

```bash
set -a && source .env && set +a

python -m ingestion.openaq backfill --country AE \
    --start 2026-07-22 --end 2026-08-30 \
    --skip-sensors-csv dbt/seeds/known_bad_sensors.csv

python -m ingestion.openaq backfill --country PK \
    --start 2026-07-22 --end 2026-08-30 \
    --skip-sensors-csv dbt/seeds/known_bad_sensors.csv
```

Each chunk fetches, lands to GCS, loads to BigQuery, and **reconciles the API
count against the BigQuery count** before checkpointing itself into
`backfill_state.json`. Interrupt freely; re-running the same command skips
completed chunks. Always pass `--skip-sensors-csv` — without it the run burns
its retry budget on ~58 sensors that have returned HTTP 500 on every request
for months.

Worked example — the 39-day recovery, run 2026-08-30: one chunk per country
over `[2026-07-22, 2026-08-30)`; AE landed 7,456 measurements (8 of 52 sensors
reporting), PK 149,678 (254 of 394); **zero failed sensors**, because the
known-bad skip list absorbed all 58; both chunks count-reconciled.

### 3. Rebuild the marts

The backfill CLI loads to BigQuery but emits no Airflow Dataset event, so the
transform doesn't self-trigger. Either trigger `openaq_transform` from the
Airflow UI, or run dbt directly in the image (the only place the pinned dbt
1.8.3 lives — **not** your host venv):

```bash
docker compose run --rm --no-deps webserver bash -c "cd /opt/airflow/dbt && dbt build"
```

### 4. Confirm the seam closed

Re-run the query from step 1, and check the recovered window has no missing
days across the join between backfilled and scheduled data:

```sql
select country_code, count(distinct measurement_date) as days
from `<project>.openaq_dbt.mart_country_compare`
where parameter = 'pm25' and measurement_date between '2026-07-22' and '2026-08-29'
group by country_code;
```

The count should equal the number of calendar days in the window. After the
2026-08-30 recovery it was 46/46 for both countries.

## Reading a run that partly failed

**A green DAG run with failed sensors is normal.** Per-sensor fetch failures are
returned as data rather than raised, and a country fails only when more than
**20%** of its sensor fetches failed. Around 30–60 Pakistani sensors return an
instant HTTP 500 on every attempt — a persistent upstream bug in contiguous
blocks of newly-onboarded sensor ids, not a transient error retries can fix. If
those turned the DAG red every night, "failed DAG" would stop being a signal.

Check `summarize_country`'s log for the failed ids. If a sensor is failing
persistently and isn't in `dbt/seeds/known_bad_sensors.csv`, add it with its
first-observed date — that list is what keeps backfills from wasting their retry
budget. The seed records first/last-observed dates rather than pretending the
set is static, because it isn't: the Phase 5 backfill surfaced 22 sensors that
daily snapshots had never caught.

**Auth failures are different.** A 401 or 403 fails the whole run immediately
and deliberately — that's an expired or revoked API key, not a data problem.

**`reconcile_counts` failing is serious.** It asserts that the measurement count
the API served equals what landed in BigQuery for that batch. If it fails, the
load is wrong; don't paper over it by re-running.

## Re-running a day

Safe, by design. The pipeline is append-only: re-running lands a second batch
with a later `ingested_at`, and staging dedups on
`(sensor_id, period_start_utc)` keeping the latest. That is also how corrected
upstream readings win. Verified live — a full duplicate run produced exactly
twice the raw rows and an unchanged staging count.

## Failure modes this project has actually hit

Each is written up with its diagnosis in `docs/PROJECT_CONTEXT.md` §7.5:

- **"Dataset not found" on a table that exists** — a BigQuery job ran in a
  different location than the dataset. `BIGQUERY_LOCATION` must equal the
  datasets' region exactly; `US` is not a superset of `us-central1`.
- **A Dataset event sits queued and the transform never starts** — DAG-file
  processors are dying, so the consumer DAG is never re-serialized. Root-owned
  `logs/scheduler/<date>/` is the known cause. Debug path:
  `dataset_event` → `dataset_dag_run_queue` → `serialized_dag.last_updated`.
  The queued event fires the moment parsing recovers.
- **Permission denied seconds after `terraform apply`** — BigQuery IAM
  propagation. Retry after a minute before assuming misconfiguration.
- **`bq query` hanging on flag parsing** — compiled dbt SQL starts with a `--`
  comment, which the CLI reads as a flag. Pipe SQL in on stdin instead.
- **A stale host venv shadowing the real environment** — the project's dbt is
  the pinned 1.8.3 inside the Airflow image. Removing a dependency from
  `pyproject.toml` does not uninstall it from an existing venv; rebuild the venv
  rather than trusting what's on the host.
