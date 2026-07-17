"""The dbt seed and ingestion/constants.py must carry the same WHO values.

From Phase 4 the seed (dbt/seeds/who_thresholds.csv) is the source of truth
dbt models read; constants.py remains for any Python-side use. The two encode
the same WHO 2021 table with one deliberate difference: constants.py stores
O3 under shorthand keys ("24h" = the 8-hour guideline, "annual" =
peak-season), while the seed's averaging_period is explicit (8h /
peak_season). This test pins that mapping so neither file can drift.
"""

import csv
from pathlib import Path

from ingestion.constants import WHO_THRESHOLD_CO_MGM3, WHO_THRESHOLDS_UGM3

SEED_PATH = Path(__file__).parents[2] / "dbt" / "seeds" / "who_thresholds.csv"

# constants.py dict key → the seed's explicit averaging_period, per pollutant
PERIOD_KEY_TO_SEED = {
    "o3": {"24h": "8h", "annual": "peak_season"},
}


def load_seed() -> dict[tuple[str, str], tuple[float, str]]:
    with SEED_PATH.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert rows, "seed file is empty"
    return {
        (r["pollutant"], r["averaging_period"]): (float(r["threshold_value"]), r["unit"])
        for r in rows
    }


def expected_from_constants() -> dict[tuple[str, str], tuple[float, str]]:
    expected = {}
    for pollutant, by_period in WHO_THRESHOLDS_UGM3.items():
        for period_key, value in by_period.items():
            if value is None:  # no 2021 guideline → no seed row
                continue
            seed_period = PERIOD_KEY_TO_SEED.get(pollutant, {}).get(period_key, period_key)
            expected[(pollutant, seed_period)] = (float(value), "µg/m³")
    for period_key, value in WHO_THRESHOLD_CO_MGM3.items():
        if value is not None:
            expected[("co", period_key)] = (float(value), "mg/m³")
    return expected


def test_seed_matches_constants_exactly():
    assert load_seed() == expected_from_constants()
