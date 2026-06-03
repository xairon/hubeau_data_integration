import numpy as np
import pandas as pd
from hubeau_pipeline.ml.indices import (
    compute_reference_grid, grid_to_zscore, grid_class_bounds,
    REF_PERIOD, CLASS_CUTOFF_PCTL,
)


def _monthly(start_year, end_year, base=100.0, noise=1.0, seed=0):
    rng = np.random.default_rng(seed)
    idx = pd.date_range(f"{start_year}-01-01", f"{end_year}-12-31", freq="MS")
    vals = base + np.sin(idx.month / 12 * 2 * np.pi) * 2 + rng.normal(0, noise, len(idx))
    return [d.strftime("%Y-%m-%d") for d in idx], list(map(float, vals))


def test_grid_normale_when_full_ref_period():
    months, values = _monthly(1991, 2020)
    res = compute_reference_grid(months, values)
    assert res["flag"] == "normale"
    assert res["baseline_start"] == "1991-01-01"
    assert res["baseline_end"] == "2020-12-31"
    # 12 months, each a 99-length grid
    assert set(res["grid"].keys()) == set(range(1, 13))
    assert all(len(res["grid"][m]) == 99 for m in range(1, 13))
    # grid is monotonic non-decreasing per month
    for m in range(1, 13):
        g = res["grid"][m]
        assert all(g[i] <= g[i + 1] + 1e-9 for i in range(len(g) - 1))


def test_grid_provisoire_when_short_record():
    months, values = _monthly(2014, 2024)  # ~10 yrs, < MIN_YEARS
    res = compute_reference_grid(months, values)
    assert res["flag"] == "provisoire"


def test_grid_adaptee_when_recent_30yr_but_not_ref():
    months, values = _monthly(2001, 2024)  # ≥15 yrs, none in 1991-2000 gap -> not full ref
    res = compute_reference_grid(months, values)
    assert res["flag"] in ("adaptee", "normale")  # ≥15 yrs in 1991-2020 portion (2001-2020) -> normale acceptable
    assert res["n_years"] >= 15


def test_zscore_monotonic_in_value():
    months, values = _monthly(1991, 2020)
    res = compute_reference_grid(months, values)
    grid_m = res["grid"][6]
    lo = grid_to_zscore(grid_m[4], grid_m)   # ~5th pctl -> negative z
    hi = grid_to_zscore(grid_m[94], grid_m)  # ~95th pctl -> positive z
    assert lo < 0 < hi


def test_class_bounds_count_and_order():
    months, values = _monthly(1991, 2020)
    res = compute_reference_grid(months, values)
    bounds = grid_class_bounds(res["grid"][6])
    # 6 cutoffs -> 6 boundary values, ascending
    assert len(bounds) == len(CLASS_CUTOFF_PCTL)
    assert all(bounds[i] <= bounds[i + 1] + 1e-9 for i in range(len(bounds) - 1))
