"""
WHO 2021 Global Air Quality Guideline thresholds.

Source: WHO 2021 Global Air Quality Guidelines (PM2.5, PM10, NO2, SO2, O3, CO).
See PROJECT_CONTEXT.md §4/G5 for the full table and rationale.

NOTE: these are the source-of-truth values for Phase 0-3. From Phase 4 onward,
the same values are loaded into BigQuery as a dbt seed (who_thresholds) so dbt
models reference the seed, not this file. Keep both in sync if either changes.
"""

WHO_THRESHOLDS_UGM3 = {
    "pm25": {"annual": 5, "24h": 15},
    "pm10": {"annual": 15, "24h": 45},
    "no2": {"annual": 10, "24h": 25},
    "so2": {"annual": None, "24h": 40},
    "o3": {"annual": 60, "24h": 100},  # annual is peak-season; 24h is 8-hour
}

# CO is reported in mg/m3, not ug/m3 — keep separate to avoid unit confusion
WHO_THRESHOLD_CO_MGM3 = {"annual": None, "24h": 4}
