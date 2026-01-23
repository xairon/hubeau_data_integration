"""
Hub'Eau Assets - Bronze Layer + CSV + dbt
"""

from .bronze import (
    piezometry_stations_raw,
    piezometry_chroniques_raw,
    piezometry_chroniques_daily_raw,  # Daily
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    hydrometry_obs_elab_raw,
    hydrometry_obs_daily_raw,  # Daily
    era5_france_timeseries_historical,  # Updated ERA5
    era5_weekly_update,  # Weekly Smart Update
)

from .csv_universal import ingest_all_csvs_asset

from .dbt_assets import hubeau_dbt_assets

all_bronze_assets = [
    piezometry_stations_raw,
    piezometry_chroniques_raw,
    piezometry_chroniques_daily_raw,
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    hydrometry_obs_elab_raw,
    hydrometry_obs_daily_raw,
    era5_france_timeseries_historical,
    era5_weekly_update,
]

all_csv_assets = [ingest_all_csvs_asset]

# dbt assets replace the manual aggregation assets
all_dbt_assets = [hubeau_dbt_assets]

all_assets = all_bronze_assets + all_csv_assets + all_dbt_assets

__all__ = ["all_assets", "all_bronze_assets", "all_csv_assets", "all_dbt_assets"]
