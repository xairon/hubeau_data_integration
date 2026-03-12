"""Chargement des données piézo et hydro depuis les tables Gold."""

import numpy as np
import pandas as pd
import psycopg2
from .config import cfg


# ── Piezo ──────────────────────────────────────────────────────────────────


def get_eligible_piezo_stations(min_days: int = 730) -> pd.DataFrame:
    """Stations piézo éligibles (≥min_days jours, dernière mesure ≥2024)."""
    query = """
        SELECT code_bss AS station_id, nature_eh, code_departement,
               COUNT(*) AS n_days, MAX(date) AS last_date
        FROM gold.hubeau_daily_chroniques
        GROUP BY code_bss, nature_eh, code_departement
        HAVING COUNT(*) >= %(min_days)s AND MAX(date) >= '2024-01-01'
        ORDER BY n_days DESC
    """
    with psycopg2.connect(cfg.dsn) as conn:
        df = pd.read_sql(query, conn, params={"min_days": min_days})
    df["domain"] = "piezo"
    return df


def get_eligible_hydro_stations(min_days: int = 730) -> pd.DataFrame:
    """Stations hydro éligibles (QmnJ, ≥min_days jours, dernière mesure ≥2024)."""
    query = """
        SELECT code_station AS station_id, type_site, code_departement, code_region,
               COUNT(*) AS n_days, MAX(date) AS last_date
        FROM gold.hydro_daily_chroniques
        WHERE grandeur_hydro_elab = 'QmnJ'
        GROUP BY code_station, type_site, code_departement, code_region
        HAVING COUNT(*) >= %(min_days)s AND MAX(date) >= '2024-01-01'
        ORDER BY n_days DESC
    """
    with psycopg2.connect(cfg.dsn) as conn:
        df = pd.read_sql(query, conn, params={"min_days": min_days})
    df["domain"] = "hydro"
    return df


# ── Sampling ───────────────────────────────────────────────────────────────


def sample_stations(eligible: pd.DataFrame, n: int, stratify_col: str,
                    seed: int | None = None) -> pd.DataFrame:
    """Échantillonnage stratifié par stratify_col."""
    seed = seed or cfg.seed
    if len(eligible) <= n:
        return eligible

    # Proportional allocation per stratum
    counts = eligible[stratify_col].value_counts()
    fracs = counts / counts.sum()
    samples = []
    for value, frac in fracs.items():
        stratum = eligible[eligible[stratify_col] == value]
        k = max(1, round(frac * n))
        k = min(k, len(stratum))
        samples.append(stratum.sample(n=k, random_state=seed))

    result = pd.concat(samples).head(n)  # trim to exact n
    return result.reset_index(drop=True)


# ── Series Loading ─────────────────────────────────────────────────────────


def load_piezo_series(station_ids: list[str]) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    """Charge les séries piézo multivar. Returns (series, dates) dicts keyed by station_id."""
    placeholders = ",".join(["%s"] * len(station_ids))
    query = f"""
        SELECT code_bss AS station_id, date,
               niveau_nappe_eau, temperature_2m, total_precipitation, potential_evaporation
        FROM gold.hubeau_daily_chroniques
        WHERE code_bss IN ({placeholders})
        ORDER BY code_bss, date
    """
    with psycopg2.connect(cfg.dsn) as conn:
        df = pd.read_sql(query, conn, params=tuple(station_ids))
    return _build_series_dicts(df, cfg.piezo_cols)


def load_hydro_series(station_ids: list[str]) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    """Charge les séries hydro multivar (QmnJ only). Returns (series, dates) dicts."""
    placeholders = ",".join(["%s"] * len(station_ids))
    query = f"""
        SELECT code_station AS station_id, date,
               resultat_obs_elab, temperature_2m, total_precipitation, potential_evaporation
        FROM gold.hydro_daily_chroniques
        WHERE code_station IN ({placeholders})
          AND grandeur_hydro_elab = 'QmnJ'
        ORDER BY code_station, date
    """
    with psycopg2.connect(cfg.dsn) as conn:
        df = pd.read_sql(query, conn, params=tuple(station_ids))
    return _build_series_dicts(df, cfg.hydro_cols)


def _build_series_dicts(df: pd.DataFrame, cols: list[str]) -> tuple[dict, dict]:
    """Convert a DataFrame with station_id, date, and value columns into series/dates dicts."""
    series = {}
    dates = {}
    for sid, group in df.groupby("station_id"):
        group = group.sort_values("date")
        arr = group[cols].values.astype(np.float32)
        # Interpolate NaN, then fill remaining with 0
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
        series[sid] = arr
        dates[sid] = group["date"].values
    return series, dates


# ── Unified Loading ────────────────────────────────────────────────────────


def load_unified_data(piezo_n: int | None = None, hydro_n: int | None = None):
    """Load and sample piezo + hydro stations. Returns (sample_df, series, dates).

    sample_df has columns: station_id, domain, + domain-specific metadata.
    series/dates are dicts keyed by station_id.
    """
    piezo_n = piezo_n or cfg.piezo_sample_size
    hydro_n = hydro_n or cfg.hydro_sample_size

    # Eligible stations
    piezo_eligible = get_eligible_piezo_stations()
    hydro_eligible = get_eligible_hydro_stations()

    print(f"  Piézo: {len(piezo_eligible)} éligibles")
    print(f"  Hydro: {len(hydro_eligible)} éligibles")

    # Sample
    piezo_sample = sample_stations(piezo_eligible, piezo_n, "nature_eh")
    hydro_sample = sample_stations(hydro_eligible, hydro_n, "type_site")

    print(f"  Piézo: {len(piezo_sample)} échantillonnées")
    print(f"  Hydro: {len(hydro_sample)} échantillonnées")

    # Load series
    piezo_series, piezo_dates = load_piezo_series(piezo_sample["station_id"].tolist())
    hydro_series, hydro_dates = load_hydro_series(hydro_sample["station_id"].tolist())

    # Merge
    sample_df = pd.concat([piezo_sample, hydro_sample], ignore_index=True)
    all_series = {**piezo_series, **hydro_series}
    all_dates = {**piezo_dates, **hydro_dates}

    print(f"  Total: {len(all_series)} séries chargées")
    return sample_df, all_series, all_dates


# ── Windowing (unchanged) ─────────────────────────────────────────────────


def make_windows(series: dict[str, np.ndarray], dates: dict[str, np.ndarray],
                 window_size: int | None = None, stride: int | None = None):
    """Découpe les séries en fenêtres glissantes."""
    window_size = window_size or cfg.window_size
    stride = stride or cfg.stride
    windowed_series = {}
    windowed_dates = {}

    for sid, arr in series.items():
        n = len(arr)
        if n < window_size:
            continue
        windows = []
        date_windows = []
        d = dates[sid]
        for start in range(0, n - window_size + 1, stride):
            windows.append(arr[start:start + window_size])
            date_windows.append((d[start], d[start + window_size - 1]))
        if windows:
            windowed_series[sid] = np.stack(windows)
            windowed_dates[sid] = date_windows

    return windowed_series, windowed_dates
