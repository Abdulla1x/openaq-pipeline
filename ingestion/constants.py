"""
WHO 2021 Global Air Quality Guideline thresholds.

Source: WHO 2021 Global Air Quality Guidelines (PM2.5, PM10, NO2, SO2, O3, CO).
See PROJECT_CONTEXT.md §4/G5 for the full table and rationale.

NOTE: since Phase 4 the dbt seed (dbt/seeds/who_thresholds.csv) is the source
of truth that models read; this module remains for Python-side use. The two
are kept in sync by tests/unit/test_who_seed_sync.py, which also pins the O3
key mapping: this dict's "24h"/"annual" shorthand for O3 corresponds to the
seed's explicit 8h/peak_season periods.
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
