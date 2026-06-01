import numpy as np
from hubeau_pipeline.ml.indices import classify_latest_spli, classify_latest_ssfi, classify_value

def test_classify_value_thresholds():
    assert classify_value(0.0) == "NORMAL"
    assert classify_value(-1.0) == "BAS"
    assert classify_value(-1.5) == "TRES_BAS"
    assert classify_value(-2.0) == "EXTREMEMENT_BAS"
    assert classify_value(2.0) == "EXTREMEMENT_HAUT"
    assert classify_value(None) == "UNKNOWN"

def test_spli_too_short_is_unknown():
    months = [f"2020-{m:02d}-01" for m in range(1, 13)]  # 12 < 60
    z, cls = classify_latest_spli(months, [1.0] * 12)
    assert z is None and cls == "UNKNOWN"

def test_spli_returns_class_on_long_series():
    months, values = [], []
    for y in range(2010, 2020):
        for m in range(1, 13):
            months.append(f"{y}-{m:02d}-01")
            values.append(10.0 + m + np.random.default_rng(y * 12 + m).normal(0, 0.5))
    months.append("2020-06-01"); values.append(0.0)  # very low June
    z, cls = classify_latest_spli(months, values)
    assert z is not None and z < 0 and cls in ("EXTREMEMENT_BAS", "TRES_BAS", "BAS")

def test_ssfi_returns_class_on_long_series():
    months, values = [], []
    for y in range(2010, 2020):
        for m in range(1, 13):
            months.append(f"{y}-{m:02d}-01")
            values.append(100.0 + m * 5 + abs(np.random.default_rng(y * 12 + m).normal(0, 3)))
    months.append("2020-06-01"); values.append(5.0)  # very low June flow
    z, cls = classify_latest_ssfi(months, values)
    assert z is not None and z < 0
