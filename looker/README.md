# looker/

Serving layer (Phase 6): the Looker Studio dashboard comparing UAE and Pakistan
against the WHO 2021 24-hour guidelines.

| File | What it is |
|---|---|
| `dashboard_spec.md` | The version-controlled source of truth for the report: data sources, controls, every chart and its metric, the finding text, and the sharing configuration. Detailed enough to rebuild the dashboard by hand. |
| `dashboard.png` | Snapshot of the published report, so the repo shows the result without requiring the reader to open the live link. |

## Why a spec and not an export

Looker Studio has **no report-as-JSON export**. Its API enumerates assets and
manages permissions; it does not serialize a report's layout, charts, or
calculated fields. "Make a copy" duplicates a report inside Looker, which is
not a diffable artifact and cannot live in git.

So the dashboard's definition is written down instead of exported. The spec is
the contract; the live report is one rendering of it. When the report changes,
the spec changes in the same commit — otherwise this directory drifts into the
same fiction the Phase 0 audit found in the scaffold READMEs.

The dashboard reads three marts in the `openaq_dbt` BigQuery dataset
(`mart_country_compare`, `mart_exceedance_summary`, `mart_annual_compare`) via
the native BigQuery connector with owner credentials — no extracts, no copies
of the data.
