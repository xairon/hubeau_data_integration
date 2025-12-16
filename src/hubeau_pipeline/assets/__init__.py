"""
Hub'Eau Assets - Bronze Layer + CSV
"""

from .bronze import (
    piezometry_stations_raw,
    piezometry_chroniques_raw,
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    hydrometry_obs_elab_raw,
    era5_france_meteo_raw,
)

from .csv_universal import ingest_all_csvs_asset

all_bronze_assets = [
    piezometry_stations_raw,
    piezometry_chroniques_raw,
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    hydrometry_obs_elab_raw,
    era5_france_meteo_raw,
]

all_csv_assets = [ingest_all_csvs_asset]
all_assets = all_bronze_assets + all_csv_assets

__all__ = ["all_assets", "all_bronze_assets", "all_csv_assets"]
