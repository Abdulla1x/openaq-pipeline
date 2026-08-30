# data/

Snapshot exports of the dbt mart tables, committed so the project's finding
stays verifiable from this repository alone — with no cloud account, no
credentials, and no dependency on the BigQuery datasets continuing to exist.

**As of: data through 2026-08-29, exported 2026-08-30.**

| File | Grain | Rows |
|---|---|---|
| `marts/mart_country_compare.csv` | (country, parameter, day) | 1,366 |
| `marts/mart_annual_compare.csv` | (country, parameter, year) | 7 |
| `marts/mart_exceedance_summary.csv` | (country, parameter) | 3 |

## Why this exists

The live dashboard reads BigQuery, and BigQuery lives in a GCP project. The
repo should not depend on that project outliving it — a reader two years from
now should still be able to check the headline claim rather than take it on
trust. These three files are small enough (168 KB total) that permanence costs
nothing.

`mart_exceedance_summary.csv` alone reproduces the finding, denominators
included, which is the point of guardrail G8: every rate sits beside the
denominator it was computed over and the span it covers.

## What is deliberately not here

- **`int_daily_aqi`** (86,391 station-day rows) — would let a reader recompute
  the marts rather than just read them, but it is megabytes rather than
  kilobytes. The marts already carry the completeness columns (G7) that the
  caveats rest on.
- **The raw zone** (1.78M measurements, ~1.2 GiB in GCS). Reproducible from the
  OpenAQ API via the backfill CLI; see the root README.

These are **outputs**, not inputs. Nothing in the pipeline reads them — dbt
builds the marts in BigQuery, and these are a point-in-time copy. They do not
update themselves, which is why the as-of date above is stated rather than
implied.

## Regenerating

```bash
for t in mart_country_compare mart_annual_compare mart_exceedance_summary; do
  bq query --use_legacy_sql=false --location=us-central1 --format=csv \
    --max_rows=100000 "select * from \`<project>.openaq_dbt.$t\` order by 1,2,3" \
    > "data/marts/$t.csv"
done
```

Update the as-of date above in the same commit — a snapshot without its date is
a claim about "now" that quietly becomes false.
