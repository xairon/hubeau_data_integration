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
