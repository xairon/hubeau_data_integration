"""Load eligible hydrological series from Gold tables for SoftCLT encoding."""

import numpy as np
import pandas as pd

PIEZO_COLS = ["niveau_nappe_eau", "temperature_2m", "total_precipitation", "potential_evaporation"]
HYDRO_COLS = ["resultat_obs_elab", "temperature_2m", "total_precipitation", "potential_evaporation"]


def _interpolate_and_fill(arr: np.ndarray) -> np.ndarray:
    """Interpolate NaN values column-wise, fill remaining with 0."""
    mask = np.isnan(arr)
    if mask.any():
        for col_idx in range(arr.shape[1]):
            col = arr[:, col_idx]
            nans = np.isnan(col)
            if nans.any() and not nans.all():
                col[nans] = np.interp(
                    np.flatnonzero(nans), np.flatnonzero(~nans), col[~nans]
                )
        arr = np.nan_to_num(arr, nan=0.0)
    return arr


def load_piezo_series(pg, min_days: int = 730) -> tuple[dict[str, np.ndarray], dict[str, list]]:
    """Load eligible piezo series from Gold.

    Returns:
        (series, dates) where series = {code_bss: (T, 4)} and dates = {code_bss: [date, ...]}
    """
    query = """
        SELECT code_bss, date,
               niveau_nappe_eau, temperature_2m,
               total_precipitation, potential_evaporation
        FROM gold.hubeau_daily_chroniques
        WHERE code_bss IN (
            SELECT code_bss
            FROM gold.hubeau_daily_chroniques
            GROUP BY code_bss
            HAVING COUNT(*) >= %(min_days)s
               AND MAX(date) >= '2024-01-01'
        )
        ORDER BY code_bss, date
    """
    with pg.get_connection() as conn:
        df = pd.read_sql(query, conn, params={"min_days": min_days})

    series = {}
    dates = {}
    for code_bss, group in df.groupby("code_bss"):
        group = group.sort_values("date")
        arr = group[PIEZO_COLS].values.astype(np.float32)
        series[code_bss] = _interpolate_and_fill(arr)
        dates[code_bss] = group["date"].tolist()
    return series, dates


def load_hydro_series(pg, min_days: int = 730) -> tuple[dict[str, np.ndarray], dict[str, list]]:
    """Load eligible hydro series (QmnJ) from Gold.

    Returns:
        (series, dates) where series = {code_station: (T, 4)} and dates = {code_station: [date, ...]}
    """
    query = """
        SELECT code_station, date,
               resultat_obs_elab, temperature_2m,
               total_precipitation, potential_evaporation
        FROM gold.hydro_daily_chroniques
        WHERE grandeur_hydro_elab = 'QmnJ'
          AND code_station IN (
            SELECT code_station
            FROM gold.hydro_daily_chroniques
            WHERE grandeur_hydro_elab = 'QmnJ'
            GROUP BY code_station
            HAVING COUNT(*) >= %(min_days)s
               AND MAX(date) >= '2024-01-01'
        )
        ORDER BY code_station, date
    """
    with pg.get_connection() as conn:
        df = pd.read_sql(query, conn, params={"min_days": min_days})

    series = {}
    dates = {}
    for code_station, group in df.groupby("code_station"):
        group = group.sort_values("date")
        arr = group[HYDRO_COLS].values.astype(np.float32)
        series[code_station] = _interpolate_and_fill(arr)
        dates[code_station] = group["date"].tolist()
    return series, dates
