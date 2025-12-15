"""
Bronze Layer Assets - Hub'Eau Raw Data Ingestion

Pattern: @asset + pipeline.run() - stable and runtime-controlled.
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

# ERA5
from hubeau_pipeline.assets.bronze.era5_assets import era5_france_meteo_raw

# Simple loader (fallback)
from hubeau_pipeline.assets.bronze.simple_assets import piezometry_chroniques_simple

__all__ = [
    "piezometry_stations_raw",
    "piezometry_chroniques_raw",
    "hydrometry_sites_raw",
    "hydrometry_stations_raw",
    "hydrometry_obs_elab_raw",
    "era5_france_meteo_raw",
    "piezometry_chroniques_simple",
    "MODE_PARTITIONS",
]
