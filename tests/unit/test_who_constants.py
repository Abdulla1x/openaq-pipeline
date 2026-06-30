from ingestion.constants import WHO_THRESHOLD_CO_MGM3, WHO_THRESHOLDS_UGM3


def test_pm25_thresholds():
    assert WHO_THRESHOLDS_UGM3["pm25"]["annual"] == 5
    assert WHO_THRESHOLDS_UGM3["pm25"]["24h"] == 15


def test_pm10_thresholds():
    assert WHO_THRESHOLDS_UGM3["pm10"]["annual"] == 15
    assert WHO_THRESHOLDS_UGM3["pm10"]["24h"] == 45


def test_no2_thresholds():
    assert WHO_THRESHOLDS_UGM3["no2"]["annual"] == 10
    assert WHO_THRESHOLDS_UGM3["no2"]["24h"] == 25


def test_co_threshold_uses_separate_dict_with_different_unit():
    # CO is mg/m3, not ug/m3 — must not be mixed into the main dict
    assert "co" not in WHO_THRESHOLDS_UGM3
    assert WHO_THRESHOLD_CO_MGM3["24h"] == 4


def test_all_pollutants_have_24h_value():
    for pollutant, values in WHO_THRESHOLDS_UGM3.items():
        assert values["24h"] is not None, f"{pollutant} missing 24h threshold"
