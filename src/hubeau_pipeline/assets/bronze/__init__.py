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

# Reference Data: TME hydrogeo entities (BDLISA)
from hubeau_pipeline.assets.bronze.tme_entites_assets import tme_entites_hydrogeo

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
    # Reference Data
    "tme_entites_hydrogeo",
    # Configs
    "MODE_PARTITIONS",
    "ERA5_PARTITIONS_DEF",
]
