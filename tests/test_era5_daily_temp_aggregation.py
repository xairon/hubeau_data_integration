"""
Tests unitaires (purs, sans réseau/DB) de l'agrégation horaire -> journalière
ERA5-Land (`aggregate_hourly_to_daily`), coeur du remplacement de
`derived-era5-land-daily-statistics` (service CADS saturé, ~43h/an) par
l'archive brute `reanalysis-era5-land` + agrégation locale mean/min/max.

Nécessite xarray/numpy/pandas réels (pas de stub) : dagster est stubbé par
`tests/conftest.py` mais reste inerte ici (le module importé ne dépend que du
décorateur `@asset`, jamais appelé par ces tests).
"""
from datetime import datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from hubeau_pipeline.assets.bronze.era5_daily_temp_assets import _days_for_month, aggregate_hourly_to_daily

LAT_RAW = np.array([45.000000001, 46.099999999])  # -> arrondi 0.1° : 45.0, 46.1
LON_RAW = np.array([2.000000001, 3.099999999])     # -> arrondi 0.1° : 2.0, 3.1


def _build_hourly_dataset(time_dim: str = "valid_time", include_number_coord: bool = True) -> xr.Dataset:
    """
    2 jours x 24h x 2x2 cellules :
    - (lat0, lon0): terre, motif croissant distinct par jour
    - (lat0, lon1): terre, motif avec Celsius négatif
    - (lat1, lon0): mer -> NaN sur toute la fenêtre (doit être éliminée)
    - (lat1, lon1): terre, valeur constante (cas dégénéré min=mean=max)
    Toutes les valeurs sont posées en Kelvin (K = degC + 273.15).
    """
    times = pd.date_range("2024-01-10", periods=48, freq="h")  # jour1: idx 0-23, jour2: idx 24-47
    day1, day2 = slice(0, 24), slice(24, 48)

    t2m_k = np.full((48, 2, 2), np.nan)

    # (lat0, lon0): terre
    t2m_k[day1, 0, 0] = np.arange(0, 24) + 273.15       # jour1 Celsius 0..23 -> mean 11.5, min 0, max 23
    t2m_k[day2, 0, 0] = np.arange(10, 34) + 273.15      # jour2 Celsius 10..33 -> mean 21.5, min 10, max 33

    # (lat0, lon1): terre, Celsius négatif
    t2m_k[day1, 0, 1] = np.arange(-5, 19) + 273.15      # mean 6.5, min -5, max 18
    t2m_k[day2, 0, 1] = np.arange(5, 29) + 273.15       # mean 16.5, min 5, max 28

    # (lat1, lon0): mer -> reste NaN

    # (lat1, lon1): terre, constant (cas dégénéré)
    t2m_k[day1, 1, 1] = 15.0 + 273.15
    t2m_k[day2, 1, 1] = 15.0 + 273.15

    coords = {
        time_dim: times,
        "latitude": LAT_RAW,
        "longitude": LON_RAW,
    }
    if include_number_coord:
        coords["number"] = 0  # coordonnée scalaire ensemble member (spike CDS) à dropper

    return xr.Dataset(
        {"t2m": ((time_dim, "latitude", "longitude"), t2m_k)},
        coords=coords,
    )


def _row(df: pd.DataFrame, lat: float, lon: float, day: str) -> pd.Series:
    match = df[(df["latitude"] == lat) & (df["longitude"] == lon) & (df["time"] == pd.Timestamp(day))]
    assert len(match) == 1, f"expected exactly 1 row for ({lat}, {lon}, {day}), got {len(match)}"
    return match.iloc[0]


def test_daily_mean_min_max_exact_per_day():
    ds = _build_hourly_dataset()
    df = aggregate_hourly_to_daily(ds)

    r1 = _row(df, 45.0, 2.0, "2024-01-10")
    assert r1["t2m_mean"] == 11.5
    assert r1["t2m_min"] == 0.0
    assert r1["t2m_max"] == 23.0

    r2 = _row(df, 45.0, 2.0, "2024-01-11")
    assert r2["t2m_mean"] == 21.5
    assert r2["t2m_min"] == 10.0
    assert r2["t2m_max"] == 33.0


def test_negative_celsius_handled():
    ds = _build_hourly_dataset()
    df = aggregate_hourly_to_daily(ds)

    r = _row(df, 45.0, 3.1, "2024-01-10")
    assert r["t2m_mean"] == 6.5
    assert r["t2m_min"] == -5.0
    assert r["t2m_max"] == 18.0


def test_degenerate_constant_cell_min_mean_max_equal():
    ds = _build_hourly_dataset()
    df = aggregate_hourly_to_daily(ds)

    r = _row(df, 46.1, 3.1, "2024-01-10")
    assert r["t2m_mean"] == r["t2m_min"] == r["t2m_max"] == 15.0


def test_sea_cell_dropped_as_nan():
    ds = _build_hourly_dataset()
    df = aggregate_hourly_to_daily(ds)

    # cellule (lat1, lon0) = mer (NaN partout) : aucune ligne ne doit exister
    assert df[(df["latitude"] == 46.1) & (df["longitude"] == 2.0)].empty


def test_row_count_is_land_cells_times_days():
    ds = _build_hourly_dataset()
    df = aggregate_hourly_to_daily(ds)

    # 3 cellules terre x 2 jours = 6 lignes (la cellule mer est éliminée)
    assert len(df) == 6


def test_coordinates_rounded_to_0_1_degree_grid():
    ds = _build_hourly_dataset()
    df = aggregate_hourly_to_daily(ds)

    assert set(df["latitude"].unique()) <= {45.0, 46.1}
    assert set(df["longitude"].unique()) <= {2.0, 3.1}


def test_kelvin_to_celsius_conversion():
    # Sanity check indépendant : une cellule à valeur K constante = 273.15 + X
    # doit ressortir en degC = X (pas K).
    ds = _build_hourly_dataset()
    df = aggregate_hourly_to_daily(ds)
    r = _row(df, 46.1, 3.1, "2024-01-11")
    assert r["t2m_mean"] == 15.0  # et non 288.15 (== 15 + 273.15, l'erreur si la conversion était oubliée)


def test_number_coord_dropped_without_error():
    ds = _build_hourly_dataset(include_number_coord=True)
    df = aggregate_hourly_to_daily(ds)
    assert "number" not in df.columns


def test_works_without_number_coord():
    ds = _build_hourly_dataset(include_number_coord=False)
    df = aggregate_hourly_to_daily(ds)
    assert len(df) == 6


def test_fallback_time_dim_when_no_valid_time():
    # Certains téléchargements CDS nomment la dim temporelle "time" au lieu de
    # "valid_time" : le fallback doit produire un résultat identique.
    ds = _build_hourly_dataset(time_dim="time")
    df = aggregate_hourly_to_daily(ds)
    assert len(df) == 6
    r = _row(df, 45.0, 2.0, "2024-01-10")
    assert r["t2m_mean"] == 11.5


def test_missing_t2m_variable_raises():
    ds = _build_hourly_dataset()
    ds = ds.rename({"t2m": "not_t2m"})
    with pytest.raises(ValueError, match="t2m"):
        aggregate_hourly_to_daily(ds)


def test_output_columns_and_time_dtype():
    ds = _build_hourly_dataset()
    df = aggregate_hourly_to_daily(ds)
    assert list(df.columns) == ["time", "latitude", "longitude", "t2m_mean", "t2m_min", "t2m_max"]
    assert pd.api.types.is_datetime64_any_dtype(df["time"])


# ============================================================================
# _days_for_month — anti-cache CADS : jours dérivés de la fenêtre réelle,
# jamais un `1..31` figé (cf. incident 2026-06-24, era5_assets.py).
# ============================================================================

def test_days_for_month_window_within_single_month():
    # Fenêtre entière contenue dans un seul mois : clampée aux deux bouts.
    start, end = datetime(2024, 3, 5), datetime(2024, 3, 20)
    days = _days_for_month(2024, 3, start, end)
    assert days == list(range(5, 21))


def test_days_for_month_window_spanning_three_months():
    start, end = datetime(2024, 1, 20), datetime(2024, 3, 10)

    first_month = _days_for_month(2024, 1, start, end)
    middle_month = _days_for_month(2024, 2, start, end)
    last_month = _days_for_month(2024, 3, start, end)

    assert first_month == list(range(20, 32))  # clampé au début (janvier = 31j)
    assert middle_month == list(range(1, 30))  # 2024 bissextile -> février complet = 29j
    assert last_month == list(range(1, 11))    # clampé à la fin


def test_days_for_month_window_ending_mid_month_nightly_update_shape():
    # Simule `era5_daily_temp_stats_update`: fenêtre ouverte au début du mois
    # en cours, se terminant "hier" (now - lag). Le dernier jour == end_date.day.
    start, end = datetime(2024, 6, 1), datetime(2024, 6, 15)
    days = _days_for_month(2024, 6, start, end)
    assert days[-1] == 15
    assert days == list(range(1, 16))


def test_days_for_month_anti_cache_property_end_date_advances():
    # LA propriété qui corrige le bug : deux fenêtres consécutives (comme deux
    # exécutions nightly successives, end_date avançant d'1 jour) DOIVENT
    # produire des listes de jours différentes -> signature de requête CDS
    # différente -> pas de cache périmé.
    start = datetime(2024, 6, 1)
    days_night_1 = _days_for_month(2024, 6, start, datetime(2024, 6, 15))
    days_night_2 = _days_for_month(2024, 6, start, datetime(2024, 6, 16))
    assert days_night_1 != days_night_2
    assert days_night_2[-1] == 16


def test_days_for_month_no_future_days_requested():
    # Le mois en cours ne doit jamais inclure de jours au-delà de end_date,
    # même si le mois calendaire a plus de jours restants.
    start, end = datetime(2024, 6, 1), datetime(2024, 6, 15)
    days = _days_for_month(2024, 6, start, end)
    assert max(days) == 15
    assert 16 not in days
    assert 30 not in days


def test_days_for_month_leap_year_february_full():
    # Mois intermédiaire d'une fenêtre multi-mois, février bissextile -> 29 jours.
    start, end = datetime(2024, 1, 15), datetime(2024, 3, 5)
    days = _days_for_month(2024, 2, start, end)
    assert days == list(range(1, 30))
    assert len(days) == 29


def test_days_for_month_non_leap_year_february_full():
    start, end = datetime(2023, 1, 15), datetime(2023, 3, 5)
    days = _days_for_month(2023, 2, start, end)
    assert days == list(range(1, 29))
    assert len(days) == 28


def test_days_for_month_single_day_window():
    start = end = datetime(2024, 6, 1)
    days = _days_for_month(2024, 6, start, end)
    assert days == [1]
