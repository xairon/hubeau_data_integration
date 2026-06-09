from hubeau_pipeline.ml.indices import compute_reference_grid, grid_to_zscore, classify_value
from hubeau_pipeline.assets.monthly_index_assets import rescore_series

def test_rescore_series_matches_grid_to_zscore_per_month():
    months = [f"{y}-{m:02d}-01" for y in range(2000, 2021) for m in range(1, 13)]
    values = [10.0 + (i % 12) * 0.1 + (i // 12) * 0.05 for i in range(len(months))]
    res = compute_reference_grid(months, values, positive_only=False)
    rows = rescore_series("piezo", "TESTCODE", months, values, res)
    last_dt = months[-1]
    import pandas as pd
    z_expected = grid_to_zscore(values[-1], res["grid"].get(pd.to_datetime(last_dt).month))
    code, type_, month, z, cls, flag = rows[-1]
    assert type_ == "piezo" and code == "TESTCODE"
    assert z == z_expected
    assert cls == (classify_value(z_expected) if z_expected is not None else "UNKNOWN")
    assert len(rows) == len(months)
