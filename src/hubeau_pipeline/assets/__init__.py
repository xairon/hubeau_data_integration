"""
Assets Hub'Eau - Bronze Layer + Reference Data

Structure:
- bronze/      : Bronze Layer - Raw data ingestion with DLT standard (22 assets)
- monitoring/  : Monitoring qualité données (1 asset)
- reference/   : SANDRE & BD-LISA reference data (17 assets)

Total: 40+ assets
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

# CSV Ingestion: Universal asset (recommended - always available)
from .csv_universal import ingest_all_csvs_asset

# CSV Ingestion: Generic assets (config-driven - optional)
from .csv_assets import csv_assets

# Legacy CSV assets (deprecated)
# from .piezometers_csv import piezometers_csv_data

# ============================================================================
# REFERENCE DATA ASSETS (SANDRE & BD-LISA)
# ============================================================================
from .bronze.sandre_raw_assets import (
    sandre_analysis_refs,
    sandre_territorial_refs,
    sandre_hydro_refs,
    sandre_intervenants,
)

from .bronze.bdlisa_raw_assets import (
    bdlisa_entites,
    bdlisa_stats,
)

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

# Universal CSV ingestion asset (always available)
universal_csv_asset = [ingest_all_csvs_asset]

# Generic CSV ingestion assets (dynamically generated from configs)
all_csv_assets = universal_csv_asset + csv_assets

# Reference data assets (SANDRE + BD-LISA)
all_reference_assets = [
    # SANDRE (4 multi-assets)
    sandre_analysis_refs,
    sandre_territorial_refs,
    sandre_hydro_refs,
    sandre_intervenants,
    # BD-LISA (2 assets)
    bdlisa_entites,
    bdlisa_stats,
]

# All assets (Bronze + Monitoring + CSV + Reference)
all_assets = all_bronze_assets + all_monitoring_assets + all_csv_assets + all_reference_assets

__all__ = [
    "all_assets",
    "all_bronze_assets",
    "all_monitoring_assets",
    "all_csv_assets",
    "all_reference_assets",
    "ingest_all_csvs_asset",
    "csv_assets",
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
    # Reference Data Assets
    "sandre_analysis_refs",
    "sandre_territorial_refs",
    "sandre_hydro_refs",
    "sandre_intervenants",
    "bdlisa_entites",
    "bdlisa_stats",
]
