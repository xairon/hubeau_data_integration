"""
Chargement et échantillonnage des séries piézo depuis Gold.

Connexion en lecture seule via psycopg2 directement.
"""

import numpy as np
import pandas as pd
import psycopg2
from typing import Dict, Tuple

from .config import cfg


def get_eligible_stations(min_days: int = 730) -> pd.DataFrame:
    """
    Retourne les stations piézo éligibles avec métadonnées.

    Critères : ≥ min_days jours de données + dernière mesure ≥ 2024-01-01.

    Returns:
        DataFrame: code_bss, n_days, first_date, last_date, nature_eh, milieu_eh
    """
    query = """
        SELECT
            code_bss,
            COUNT(*) AS n_days,
            MIN(date) AS first_date,
            MAX(date) AS last_date,
            nature_eh,
            milieu_eh
        FROM gold.hubeau_daily_chroniques
        GROUP BY code_bss, nature_eh, milieu_eh
        HAVING COUNT(*) >= %(min_days)s
           AND MAX(date) >= '2024-01-01'
        ORDER BY n_days DESC
    """
    with psycopg2.connect(cfg.dsn) as conn:
        return pd.read_sql(query, conn, params={"min_days": min_days})


def sample_stations(eligible: pd.DataFrame, n: int | None = None, seed: int | None = None) -> pd.DataFrame:
    """
    Échantillonnage stratifié par nature_eh (proportionnel).
    """
    n = n or cfg.sample_size
    seed = seed or cfg.seed
    rng = np.random.default_rng(seed)

    if len(eligible) <= n:
        return eligible.reset_index(drop=True)

    strata = eligible.groupby("nature_eh", observed=True)
    proportions = strata.size() / len(eligible)

    sampled = []
    for nature, group in strata:
        k = max(1, int(round(proportions[nature] * n)))
        k = min(k, len(group))
        idx = rng.choice(len(group), size=k, replace=False)
        sampled.append(group.iloc[idx])

    result = pd.concat(sampled)

    if len(result) > n:
        result = result.sample(n=n, random_state=seed)
    elif len(result) < n:
        remaining = eligible[~eligible.code_bss.isin(result.code_bss)]
        extra = remaining.sample(n=min(n - len(result), len(remaining)), random_state=seed)
        result = pd.concat([result, extra])

    return result.reset_index(drop=True)


def load_series(station_ids: list[str]) -> Tuple[Dict[str, np.ndarray], Dict[str, list]]:
    """
    Charge les séries multivariate pour les stations données.

    Returns:
        series: {code_bss: np.ndarray shape (T, 4) float32}
        dates:  {code_bss: [date, ...]}

    Les NaN sont interpolés linéairement puis remplis à 0.
    """
    placeholders = ",".join(["%s"] * len(station_ids))
    query = f"""
        SELECT code_bss, date,
               niveau_nappe_eau, temperature_2m,
               total_precipitation, potential_evaporation
        FROM gold.hubeau_daily_chroniques
        WHERE code_bss IN ({placeholders})
        ORDER BY code_bss, date
    """
    with psycopg2.connect(cfg.dsn) as conn:
        df = pd.read_sql(query, conn, params=tuple(station_ids))

    series = {}
    dates = {}
    for code_bss, group in df.groupby("code_bss"):
        arr = group[cfg.piezo_cols].interpolate().fillna(0).values.astype(np.float32)
        series[code_bss] = arr
        dates[code_bss] = group["date"].tolist()

    return series, dates


def make_windows(
    series: Dict[str, np.ndarray],
    dates: Dict[str, list],
    window_size: int | None = None,
    stride: int | None = None,
) -> Tuple[Dict[str, np.ndarray], Dict[str, list]]:
    """
    Découpe chaque série en fenêtres glissantes.

    Returns:
        windowed: {code_bss: np.ndarray shape (n_windows, window_size, n_vars)}
        win_dates: {code_bss: [(start_date, end_date), ...]}
    """
    window_size = window_size or cfg.window_size
    stride = stride or cfg.stride

    windowed = {}
    win_dates = {}
    for bss, arr in series.items():
        T = len(arr)
        if T < window_size:
            continue
        windows = []
        wdates = []
        for start in range(0, T - window_size + 1, stride):
            end = start + window_size
            windows.append(arr[start:end])
            d = dates.get(bss, [])
            if d:
                wdates.append((str(d[start]), str(d[end - 1])))
        windowed[bss] = np.stack(windows)
        win_dates[bss] = wdates
    return windowed, win_dates
