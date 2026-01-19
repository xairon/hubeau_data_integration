"""
Bronze Layer Assets - Hub'Eau Raw Data Ingestion

Uses official dagster-dlt pattern with @dlt_assets and @dlt.source.
"""

from hubeau_pipeline.assets.bronze.dlt_assets import (
    # Piezometry
    piezometry_stations_raw,
    piezometry_chroniques_raw,
    # Hydrometry
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    hydrometry_obs_elab_raw,
    # Partitions
    MODE_PARTITIONS,
)

# ERA5 Meteorology
from hubeau_pipeline.assets.bronze.era5_assets import (
    era5_france_meteo_raw,
    era5_france_timeseries,
    ERA5_PARTITIONS_DEF,
)

__all__ = [
    "piezometry_stations_raw",
    "piezometry_chroniques_raw",
    "hydrometry_sites_raw",
    "hydrometry_stations_raw",
    "hydrometry_obs_elab_raw",
    "era5_france_meteo_raw",
    "era5_france_timeseries",
    "MODE_PARTITIONS",
    "ERA5_PARTITIONS_DEF",
]
