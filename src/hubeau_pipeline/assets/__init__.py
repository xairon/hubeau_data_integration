"""
Hub'Eau Assets - Bronze Layer
"""

from .bronze import (
    piezometry_stations_raw,
    piezometry_chroniques_raw,
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    hydrometry_obs_elab_raw,
    era5_france_meteo_raw,
    piezometry_chroniques_simple,
)

from .monitoring import all_monitoring_assets
from .csv_universal import ingest_all_csvs_asset
from .csv_assets import csv_assets

all_bronze_assets = [
    piezometry_stations_raw,
    piezometry_chroniques_raw,
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    hydrometry_obs_elab_raw,
    era5_france_meteo_raw,
    piezometry_chroniques_simple,
]

all_csv_assets = [ingest_all_csvs_asset] + csv_assets
all_assets = all_bronze_assets + all_monitoring_assets + all_csv_assets

__all__ = ["all_assets", "all_bronze_assets", "all_monitoring_assets", "all_csv_assets"]
