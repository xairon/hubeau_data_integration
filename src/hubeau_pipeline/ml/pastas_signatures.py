"""Pastas groundwater signatures and Dutch statistics computation.

Computes 30 groundwater signatures via ps.stats.signatures.summary()
and 7 Dutch statistics (GHG/GLG/GVG) via ps.stats.dutch.*.

No Pastas model fitting required — operates on raw GWL series.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

try:
    import pastas as ps

    PASTAS_AVAILABLE = True
except ImportError:
    PASTAS_AVAILABLE = False

SIGNATURE_NAMES = [
    "cv_period_mean",
    "cv_date_min",
    "cv_date_max",
    "cv_fall_rate",
    "cv_rise_rate",
    "parde_seasonality",
    "avg_seasonal_fluctuation",
    "interannual_variation",
    "low_pulse_count",
    "high_pulse_count",
    "low_pulse_duration",
    "high_pulse_duration",
    "bimodality_coefficient",
    "mean_annual_maximum",
    "rise_rate",
    "fall_rate",
    "reversals_avg",
    "reversals_cv",
    "colwell_contingency",
    "colwell_constancy",
    "recession_constant",
    "recovery_constant",
    "duration_curve_slope",
    "duration_curve_ratio",
    "richards_pathlength",
    "baselevel_index",
    "baselevel_stability",
    "magnitude",
    "autocorr_time",
    "date_min",
    "date_max",
]

DUTCH_STATS = [
    ("gg", "gg"),
    ("ghg", "ghg"),
    ("glg", "glg"),
    ("gvg", "gvg"),
    ("q_ghg", "q_ghg"),
    ("q_glg", "q_glg"),
    ("q_gvg", "q_gvg"),
]


def compute_signatures_single(code_bss: str, gwl: pd.Series) -> dict:
    """Compute 30 groundwater signatures + 7 Dutch stats for one station."""
    if not PASTAS_AVAILABLE:
        return _failure_result(code_bss, gwl, "pastas not installed")

    if len(gwl) == 0 or gwl.isna().all():
        return _failure_result(code_bss, gwl, "empty or all-NaN series")

    valid = gwl.dropna()
    base_meta = {
        "code_bss": code_bss,
        "series_start": gwl.index.min().date(),
        "series_end": gwl.index.max().date(),
        "series_length_days": (gwl.index.max() - gwl.index.min()).days,
        "n_valid_days": len(valid),
    }

    result = {**base_meta}

    try:
        sigs_df = ps.stats.signatures.summary(gwl)
        sigs = sigs_df.iloc[:, 0].to_dict()
        result.update(sigs)
        result["n_signatures_computed"] = sum(1 for v in sigs.values() if pd.notna(v))
    except Exception as e:
        for name in SIGNATURE_NAMES:
            result[name] = None
        result["n_signatures_computed"] = 0
        result["success"] = False
        result["error"] = f"signatures failed: {e}"
        return result

    for col_name, func_name in DUTCH_STATS:
        try:
            func = getattr(ps.stats, func_name)
            result[col_name] = float(func(gwl))
        except Exception:
            result[col_name] = None

    result["success"] = True
    return result


def _failure_result(code_bss: str, gwl: pd.Series, error: str) -> dict:
    result: dict = {"code_bss": code_bss, "success": False, "error": error}
    if len(gwl) > 0 and not gwl.index.empty:
        result["series_start"] = gwl.index.min().date()
        result["series_end"] = gwl.index.max().date()
        result["series_length_days"] = (gwl.index.max() - gwl.index.min()).days
        result["n_valid_days"] = int(gwl.notna().sum())
    else:
        result["series_start"] = None
        result["series_end"] = None
        result["series_length_days"] = 0
        result["n_valid_days"] = 0
    result["n_signatures_computed"] = 0
    for name in SIGNATURE_NAMES:
        result[name] = None
    for col_name, _ in DUTCH_STATS:
        result[col_name] = None
    return result
