"""
Assets Hub'Eau - Bronze Layer Only

Structure:
- bronze/      : Bronze Layer - Raw data ingestion with DLT standard (22 assets)
- monitoring/  : Monitoring qualité données (1 asset)

Total: 23 assets
"""

# ============================================================================
# BRONZE LAYER ASSETS (DLT Standard)
# ============================================================================
from .bronze import (
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

# Monitoring assets
from .monitoring import all_monitoring_assets

# ============================================================================
# ALL ASSETS
# ============================================================================
all_bronze_assets = [
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
]

# All assets (Bronze + Monitoring)
all_assets = all_bronze_assets + all_monitoring_assets

__all__ = [
    "all_assets",
    "all_bronze_assets",
    "all_monitoring_assets",
    # Bronze Layer Assets
    "temperature_stations_raw",
    "temperature_chroniques_raw",
    "piezometry_stations_raw",
    "piezometry_chroniques_raw",
    "hydrometry_sites_raw",
    "hydrometry_stations_raw",
    "hydrometry_obs_elab_raw",
    "hydrobio_stations_raw",
    "hydrobio_indices_raw",
    "hydrobio_taxons_raw",
    "quality_rivers_stations_raw",
    "quality_rivers_analyses_raw",
    "quality_rivers_conditions_raw",
    "quality_rivers_operations_raw",
    "quality_groundwater_stations_raw",
    "quality_groundwater_analyses_raw",
    "ecoulement_stations_raw",
    "ecoulement_campagnes_raw",
    "ecoulement_observations_raw",
    "prelevements_ouvrages_raw",
    "prelevements_points_raw",
    "prelevements_chroniques_raw",
]
