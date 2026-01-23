"""
Bronze Layer Assets - Hub'Eau Raw Data Ingestion
"""

from hubeau_pipeline.assets.bronze.dlt_assets import (
    # Piezometry
    piezometry_stations_raw,
    piezometry_chroniques_raw,
    piezometry_chroniques_daily_raw,
    # Hydrometry
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    hydrometry_obs_elab_raw,
    hydrometry_obs_daily_raw,
    # Partitions
    MODE_PARTITIONS,
)

# ERA5 Meteorology
from hubeau_pipeline.assets.bronze.era5_assets import (
    era5_france_timeseries_historical,
    era5_weekly_update,
    ERA5_PARTITIONS_DEF,
)

__all__ = [
    # Hub'Eau Historical
    "piezometry_stations_raw",
    "piezometry_chroniques_raw",
    "hydrometry_sites_raw",
    "hydrometry_stations_raw",
    "hydrometry_obs_elab_raw",
    # Hub'Eau Daily
    "piezometry_chroniques_daily_raw",
    "hydrometry_obs_daily_raw",
    # ERA5
    "era5_france_timeseries_historical",
    "era5_weekly_update",
    # Configs
    "MODE_PARTITIONS",
    "ERA5_PARTITIONS_DEF",
]
