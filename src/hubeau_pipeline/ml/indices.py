"""Standardized hydrological indices (latest-month classification).

Mirrors time-serie-explo/dashboard/utils/drought.py (BRGM IPS/Meteo-France
methodology). IPS/SPLI = KDE->normal for groundwater levels; SSFI = gamma->normal
for river streamflow. 7 classes from the standardized value z.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats

_THRESHOLDS_7 = [
    (-float("inf"), -1.75, "EXTREMEMENT_BAS"),
    (-1.75, -1.28, "TRES_BAS"),
    (-1.28, -0.84, "BAS"),
    (-0.84, 0.84, "NORMAL"),
    (0.84, 1.28, "HAUT"),
    (1.28, 1.75, "TRES_HAUT"),
    (1.75, float("inf"), "EXTREMEMENT_HAUT"),
]
MIN_MONTHS = 60       # 5 years minimum
MIN_PER_MONTH = 10    # min observations per calendar month for fitting


def classify_value(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "UNKNOWN"
    for lo, hi, label in _THRESHOLDS_7:
        if lo <= value < hi:
            return label
    return "EXTREMEMENT_HAUT"


def classify_latest_spli(months: list[str], values: list[float]) -> tuple[float | None, str]:
    """IPS/SPLI for the most recent month (KDE -> standard normal)."""
    if len(months) < MIN_MONTHS:
        return None, "UNKNOWN"
    series = pd.Series(values, index=pd.to_datetime(months), dtype=float).dropna()
    if len(series) < MIN_MONTHS:
        return None, "UNKNOWN"
    grouped = series.groupby(series.index.month)
    last_month = series.index[-1].month
    last_val = float(series.iloc[-1])
    if last_month not in grouped.groups or len(grouped.get_group(last_month)) < MIN_PER_MONTH:
        return None, "UNKNOWN"
    try:
        kde = stats.gaussian_kde(grouped.get_group(last_month).values)
    except Exception:
        return None, "UNKNOWN"
    cdf_val = float(np.clip(kde.integrate_box_1d(-np.inf, last_val), 0.001, 0.999))
    z = round(float(stats.norm.ppf(cdf_val)), 3)
    return z, classify_value(z)


def classify_latest_ssfi(months: list[str], values: list[float]) -> tuple[float | None, str]:
    """SSFI for the most recent month (gamma -> standard normal)."""
    if len(months) < MIN_MONTHS:
        return None, "UNKNOWN"
    series = pd.Series(values, index=pd.to_datetime(months), dtype=float).dropna()
    valid = series[series > 0]
    if len(valid) < MIN_MONTHS:
        return None, "UNKNOWN"
    grouped = valid.groupby(valid.index.month)
    last_month = valid.index[-1].month
    last_val = float(valid.iloc[-1])
    if last_month not in grouped.groups or len(grouped.get_group(last_month)) < MIN_PER_MONTH:
        return None, "UNKNOWN"
    group = grouped.get_group(last_month)
    try:
        a, loc, scale = stats.gamma.fit(group.values, floc=0)
        cdf_val = stats.gamma.cdf(last_val, a, loc=loc, scale=scale)
    except Exception:
        return None, "UNKNOWN"
    cdf_val = float(np.clip(cdf_val, 0.001, 0.999))
    z = round(float(stats.norm.ppf(cdf_val)), 3)
    return z, classify_value(z)


# ---- Fixed-reference IPS grid (BRGM-aligned, empirical percentiles) ----

REF_PERIOD = (1991, 2020)          # WMO/BRGM climatological normal (configurable)
MIN_YEARS = 15                     # BRGM minimum for statistical validity
PCTL_GRID = list(range(1, 100))    # store percentiles 1..99
# 7-class boundaries as CDF percentiles = 100 * norm.cdf([-1.75,-1.28,-0.84,0.84,1.28,1.75])
CLASS_CUTOFF_PCTL = [4.01, 10.03, 20.05, 79.95, 89.97, 95.99]


def _select_reference_window(series, ref_period=REF_PERIOD, min_years=MIN_YEARS):
    """Choose the reference window per the fallback ladder.

    Returns (windowed_series, baseline_start, baseline_end, flag, n_years).
    """
    lo, hi = ref_period
    win = series[(series.index.year >= lo) & (series.index.year <= hi)]
    if win.index.year.nunique() >= min_years:
        return win, f"{lo}-01-01", f"{hi}-12-31", "normale", int(win.index.year.nunique())

    # Best decade-aligned 30-yr window with the most years, requiring >= min_years
    first_decade = (int(series.index.year.min()) // 10) * 10
    last_decade = (int(series.index.year.max()) // 10) * 10
    best = None
    for start in range(first_decade, last_decade + 1, 10):
        w = series[(series.index.year >= start) & (series.index.year <= start + 29)]
        ny = w.index.year.nunique()
        if ny >= min_years and (best is None or ny > best[4]):
            best = (w, f"{start}-01-01", f"{start + 29}-12-31", "adaptee", int(ny))
    if best is not None:
        return best

    # Fallback: full record
    return (series, str(series.index.min().date()),
            str(series.index.max().date()), "provisoire", int(series.index.year.nunique()))


def compute_reference_grid(months, values, ref_period=REF_PERIOD,
                           min_years=MIN_YEARS, min_per_month=MIN_PER_MONTH,
                           positive_only=False):
    """Per-calendar-month empirical percentile grid over a fixed reference window.

    Args:
        months: list of ISO date strings (monthly series).
        values: monthly mean values (m NGF for piezo, m3/s for hydro).
        positive_only: if True (streamflow), drop non-positive values.

    Returns dict: {grid: {month: [99 floats]}, baseline_start, baseline_end, flag, n_years}.
    Months with < min_per_month observations are linearly interpolated from neighbours;
    if none available, that month maps to None.

    Note on thin records: if no calendar month has >= min_per_month observations, every
    month maps to None (insufficient data for a reference — those stations correctly get
    no IPS). The flag still reflects the window chosen (typically "provisoire").
    Do NOT fabricate a grid in this case.
    """
    series = pd.Series(values, index=pd.to_datetime(months), dtype=float).dropna()
    if positive_only:
        series = series[series > 0]
    if series.empty:
        return {"grid": {m: None for m in range(1, 13)},
                "baseline_start": None, "baseline_end": None, "flag": "provisoire", "n_years": 0}

    win, b_start, b_end, flag, n_years = _select_reference_window(series, ref_period, min_years)

    grid = {}
    for m in range(1, 13):
        vals = win[win.index.month == m].values
        if len(vals) >= min_per_month:
            grid[m] = [float(np.percentile(vals, p)) for p in PCTL_GRID]
        else:
            grid[m] = None

    # Interpolate missing months from nearest available neighbours (circular)
    available = {m: g for m, g in grid.items() if g is not None}
    if available:
        for m in range(1, 13):
            if grid[m] is None:
                # nearest neighbour by circular month distance
                nearest = min(available.keys(),
                              key=lambda k: min(abs(k - m), 12 - abs(k - m)))
                grid[m] = available[nearest]

    return {"grid": grid, "baseline_start": b_start, "baseline_end": b_end,
            "flag": flag, "n_years": n_years}


def grid_to_zscore(value, grid_month):
    """Standardize a value against a month's percentile grid (empirical CDF -> normal)."""
    if grid_month is None or value is None or pd.isna(value):
        return None
    # interpolate the percentile rank of `value` within the grid (1..99 -> CDF)
    pct = float(np.interp(value, grid_month, PCTL_GRID)) / 100.0
    pct = float(np.clip(pct, 0.001, 0.999))
    return round(float(stats.norm.ppf(pct)), 3)


def grid_class_bounds(grid_month):
    """Return the 6 class-boundary values (physical units) at the BRGM cutoffs."""
    if grid_month is None:
        return None
    return [float(np.interp(c, PCTL_GRID, grid_month)) for c in CLASS_CUTOFF_PCTL]
