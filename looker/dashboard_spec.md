# Dashboard spec — UAE vs Pakistan air quality

**Live report:** https://datastudio.google.com/reporting/7c88238c-c153-4c64-bf5b-c1974f859158/page/9Fj4F

Looker Studio has no report-as-JSON export (its API enumerates assets, it does
not serialize layout). This file is the version-controlled source of truth: it
describes the dashboard well enough to rebuild it by hand, and `dashboard.png`
is a snapshot of the result. See `README.md` for why it's a spec, not an export.

All figures below are the state at publish (data through **2026-08-29**), from
the marts in BigQuery dataset `openaq_dbt`; they move as the daily pipeline
appends days. The dashboard itself is always live against the marts — only this
file's quoted numbers are a point-in-time snapshot.

---

## The finding (headline text on the report)

> Over the 15 months both countries report in common (since Jun 2025),
> Pakistan's country-average PM2.5 exceeded the WHO 24-hour guideline
> (15 µg/m³) on **every single day** (100% of 455 days; mean 75 µg/m³, ~5× the
> guideline), versus the UAE's **94%** of days at roughly half the level (mean
> 37 µg/m³, ~2.5×). Both breach the guideline almost always — the gap is
> magnitude, not frequency — but the UAE figure rests on ~8 reporting stations a
> day to Pakistan's ~180, none of them reference monitors in this window, and
> neither country has usable NO₂ (PK has zero NO₂ sensors) or in-window PM10
> coverage to compare.

## Data sources (BigQuery connector, owner credentials)

Three tables in `openaq-pipeline.openaq_dbt`, each added as its own data source:

| Source | Table | Feeds |
|---|---|---|
| A | `mart_country_compare` | time-series charts, coverage-over-time |
| B | `mart_exceedance_summary` | scorecards, summary table |
| C | `mart_annual_compare` | annual table |

Freshness: default (12h) is fine — the pipeline is daily batch.

## Series colours (keep consistent everywhere)

- **UAE (AE)** — one fixed colour (e.g. teal `#1f9e89`)
- **Pakistan (PK)** — one fixed colour (e.g. amber/orange `#e08214`)
- **WHO threshold line** — neutral grey, dashed, thin (a reference, not a series)

Same two country colours on every chart so the eye reads country once.

## Controls (top of page)

- **Parameter** drop-down control on source A (`parameter`), default `pm25`.
  Observed live: it filters charts on the *other* marts too, wherever the field
  name matches — the annual table follows it and shows 5 of its 7 rows on the
  default view (the two hidden rows are AE pm10). That is consistent behaviour,
  not a bug: with the control on `pm25` the whole page is a PM2.5 page. Switch
  it to `pm10` to surface AE's dormant reference series.
- **Date range** control, default **2024-07-01 → today** — the full span, not
  the common window. The headline rate is already common-window-restricted in
  the mart (`days_exceedance_rate_common`), so the charts are free to show all
  the history there is, and the AE-only 2024–25 stretch is visible rather than
  hidden behind a caption telling the reader to widen the range. The two
  pre-aggregated tables carry a **Custom** chart-level date range covering the
  full span, so the control cannot truncate their numbers.

## Layout — one page, three bands

### Band 1 — headline
1. **Title** + the finding text box (above).
2. **Scorecard row**, source B, each with a chart-level filter:
   - PK PM2.5 exceedance-days rate — `days_exceedance_rate_common`, filter
     `country_code = PK AND parameter = pm25` → 100% (455/455 days)
   - AE PM2.5 exceedance-days rate — same, `country_code = AE` → 94% (426/455)
   - PK mean daily PM2.5 — `mean_country_daily_avg`, PK/pm25 → 75 µg/m³
   - AE mean daily PM2.5 — AE/pm25 → 37 µg/m³
   Label the rate scorecards "% of days over WHO 24h guideline (common window,
   since Jun 2025)".

   **Window caveat (recorded in the Phase 7 audit).** The two rates are
   common-window columns; `mean_country_daily_avg` is **full-span** — the model
   publishes no common-window mean. For PK the two coincide (PK's own first day
   *is* the common-span start), and for AE they agree to the quoted figure
   (full-span 37.4 vs common-window 36.8 µg/m³, both "37"), so the headline
   sentence is accurate as written. It is accurate by a margin, not by
   construction: if AE's pre-2025-06 stretch ever diverges from its later
   readings, these two scorecards start describing different windows while
   sitting in the same row. Recompute both before requoting them.
3. **Summary table**, source B, all rows — the G8 artifact (every rate beside its
   denominator and span). Columns: `country_code`, `parameter`,
   `first_day_with_data`, `last_day_with_data`, `days_exceeded`,
   `days_comparable`, `days_exceedance_rate`, `station_days_exceedance_rate`.

### Band 2 — trends
4. **PM2.5 daily average vs guideline** — time series, source A. Dimension
   `measurement_date`; metric `AVG(rolling_7d_avg)`; breakdown `country_code`.
   Reference line: type **Data**, metric `AVG(threshold_24h)`, dashed grey
   (tracks the parameter control automatically). Fallback if the data reference
   line misbehaves: constant line at 15 on a pm25-pinned chart — note which was
   used here: _______.
5. **Share of stations exceeding, 7-day** — time series, source A. Metric
   `AVG(rolling_7d_station_exceedance_rate)`, breakdown `country_code`, y-axis
   fixed 0–100. The daily `exceedance_rate` was plotted here first and is
   unreadable — a dense 0–100 spike field on both countries, because exceedance
   is close to binary day to day. The mart column smooths it over the same
   7-calendar-day range window as `rolling_7d_avg`, as a **ratio of sums**
   (Σ exceeding ÷ Σ comparable), not a mean of the daily rates: AE reports ~8
   stations a day to PK's ~180, so averaging rates would weight a 3-station day
   equal to a 180-station one. Directly beneath, sharing its
   x-extent, sits the stations-per-day chart (item 6), which doubles as the
   denominator behind this rate (G8).

   The spec originally called for a separate thin `locations_comparable`
   series here. Dropped as a duplicate: `locations_comparable` and
   `locations_with_data` are equal on **all 1,366 country-days** in the marts
   — every station-day with data has a matching 24h threshold, because both
   parameters that actually landed data (pm25, pm10) have µg/m³ guidelines.
   The two would have been the same line drawn twice. Item 6 is retitled to
   carry both readings. (The columns are *not* redundant in the model — a
   pollutant with no 24h guideline in its unit would separate them, which is
   exactly the case G8's denominator exists to keep visible.)

### Band 3 — coverage & caveats
6. **Stations reporting per day — the denominator behind the rate above** —
   time series, source A, metric `MAX(locations_with_data)` by date, breakdown
   `country_code`, linear y-axis. Positioned directly under chart 5 on a shared
   x-extent (see item 5). The AE≈8 vs PK≈180 gap is itself the story — don't
   log-scale it away.
7. **Coverage summary table** — source A, aggregated: `country_code` →
   `MAX(locations_with_data)`, `AVG(avg_hours_covered)`,
   `MAX(reference_monitor_locations)`.
8. **Annual means table** — source C, all rows: `country_code`, `parameter`,
   `measurement_year`, `annual_mean`, `threshold_annual`, `exceeds_annual`,
   `days_with_data`, `locations_with_data`.
9. **Caveats text box** (the honesty block, G7):
   - PK has **zero NO₂ sensors** — the NO₂ comparison is empirically empty on
     both sides.
   - AE's reference network went largely dormant ~Jan 2025 (AE PM10 data ends
     2025-01-13). In the common window AE's PM2.5 comes from **zero** reference
     monitors — every reporting AE station is a non-reference sensor — so the
     "sparse-but-instrumented UAE" framing needs that asterisk.
   - Spans differ: AE from 2024-07, PK from 2025-06 — headline uses the common
     window since Jun 2025.
   - Completeness is shown, never filtered (thin-coverage days stay in).
   - Footer: data © OpenAQ v3 · thresholds WHO 2021 · source
     github.com/Abdulla1x/openaq-pipeline

## Sharing

Share → General access → **Anyone with the link · Viewer** (set 2026-08-30 and
verified from a logged-out incognito window — a report that still prompts for a
Google sign-in is invisible to whoever you send it to, and the share dialog
alone does not prove otherwise). Data sources keep owner credentials; viewers'
queries bill the owner's BigQuery free tier (marts are a few MB — negligible
against 1 TB/month).
