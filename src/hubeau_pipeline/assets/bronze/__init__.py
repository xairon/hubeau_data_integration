"""
Bronze Layer Assets - Hub'Eau Raw Data Ingestion

This module contains all DLT assets for ingesting raw data from Hub'Eau API
into PostgreSQL Bronze layer (_raw tables).

Architecture:
- 22 assets total (8 stations + 14 chroniques/observations)
- DLT standard postgres destination
- No PK/FK constraints (duplicates allowed)
- Transformations deferred to Silver layer (dbt)
"""

from hubeau_pipeline.assets.bronze.dlt_assets import (
    # Temperature (2)
    temperature_stations_raw,
    temperature_chroniques_raw,

    # Piezometry (2)
    piezometry_stations_raw,
    piezometry_chroniques_raw,

    # Hydrometry (3)
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    hydrometry_obs_elab_raw,

    # Hydrobiology (3)
    hydrobio_stations_raw,
    hydrobio_indices_raw,
    hydrobio_taxons_raw,

    # Quality Rivers (4)
    quality_rivers_stations_raw,
    quality_rivers_analyses_raw,
    quality_rivers_conditions_raw,
    quality_rivers_operations_raw,

    # Quality Groundwater (2)
    quality_groundwater_stations_raw,
    quality_groundwater_analyses_raw,

    # Ecoulement (3)
    ecoulement_stations_raw,
    ecoulement_campagnes_raw,
    ecoulement_observations_raw,

    # Prelevements (3)
    prelevements_ouvrages_raw,
    prelevements_points_raw,
    prelevements_chroniques_raw,
)

# ERA5 (1)
from hubeau_pipeline.assets.bronze.era5_assets import (
    era5_france_meteo_raw,
)

__all__ = [
    # Temperature
    "temperature_stations_raw",
    "temperature_chroniques_raw",

    # Piezometry
    "piezometry_stations_raw",
    "piezometry_chroniques_raw",

    # Hydrometry
    "hydrometry_sites_raw",
    "hydrometry_stations_raw",
    "hydrometry_obs_elab_raw",

    # Hydrobiology
    "hydrobio_stations_raw",
    "hydrobio_indices_raw",
    "hydrobio_taxons_raw",

    # Quality Rivers
    "quality_rivers_stations_raw",
    "quality_rivers_analyses_raw",
    "quality_rivers_conditions_raw",
    "quality_rivers_operations_raw",

    # Quality Groundwater
    "quality_groundwater_stations_raw",
    "quality_groundwater_analyses_raw",

    # Ecoulement
    "ecoulement_stations_raw",
    "ecoulement_campagnes_raw",
    "ecoulement_observations_raw",

    # Prelevements
    "prelevements_ouvrages_raw",
    "prelevements_points_raw",
    "prelevements_chroniques_raw",

    # ERA5
    "era5_france_meteo_raw",
]
