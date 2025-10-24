"""
Assets Hub'Eau - Ingestion directe PostgreSQL

Structure:
- hubeau_assets.py      : Ingestion des données Hub'Eau
- monitoring/           : Monitoring qualité données
- schema_optimization.py: Optimisation intelligente des schémas PostgreSQL
"""

from .hubeau_assets import (
    # Chroniques/Observations (avec filtres date)
    piezometry_chroniques_csv,
    quality_groundwater_analyses_csv,
    quality_rivers_analyses_csv,
    quality_rivers_conditions_csv,
    quality_rivers_operations_csv,
    temperature_chroniques_csv,
    hydrometry_obs_elab_csv,
    hydrobio_indices_csv,
    hydrobio_taxons_csv,
    ecoulement_observations_csv,
    prelevements_chroniques_csv,
    # Stations/Référentiels (sans filtres date)
    piezometry_stations_csv,
    quality_groundwater_stations_csv,
    quality_rivers_stations_csv,
    temperature_stations_csv,
    hydrometry_sites_csv,
    hydrometry_stations_csv,
    hydrobio_stations_csv,
    ecoulement_stations_csv,
    ecoulement_campagnes_csv,
    prelevements_points_csv,
    prelevements_ouvrages_csv,
)
from .monitoring import all_monitoring_assets
from .schema_optimization import optimize_hubeau_schema, analyze_table_schema

# Tous les assets Hub'Eau
all_hubeau_assets = [
    # Chroniques/Observations
    piezometry_chroniques_csv,
    quality_groundwater_analyses_csv,
    quality_rivers_analyses_csv,
    quality_rivers_conditions_csv,
    quality_rivers_operations_csv,
    temperature_chroniques_csv,
    hydrometry_obs_elab_csv,
    hydrobio_indices_csv,
    hydrobio_taxons_csv,
    ecoulement_observations_csv,
    prelevements_chroniques_csv,
    # Stations/Référentiels
    piezometry_stations_csv,
    quality_groundwater_stations_csv,
    quality_rivers_stations_csv,
    temperature_stations_csv,
    hydrometry_sites_csv,
    hydrometry_stations_csv,
    hydrobio_stations_csv,
    ecoulement_stations_csv,
    ecoulement_campagnes_csv,
    prelevements_points_csv,
    prelevements_ouvrages_csv,
]

# Assets d'optimisation de schéma
all_schema_assets = [
    optimize_hubeau_schema,
    analyze_table_schema,
]

# Tous les assets du pipeline
all_assets = all_hubeau_assets + all_monitoring_assets + all_schema_assets

__all__ = [
    "all_assets",
    "all_hubeau_assets",
    "all_monitoring_assets",
    "all_schema_assets",
    # Schema optimization
    "optimize_hubeau_schema",
    "analyze_table_schema",
    # Individual assets
    "piezometry_chroniques_csv",
    "quality_groundwater_analyses_csv",
    "quality_rivers_analyses_csv",
    "quality_rivers_conditions_csv",
    "quality_rivers_operations_csv",
    "temperature_chroniques_csv",
    "hydrometry_obs_elab_csv",
    "hydrobio_indices_csv",
    "hydrobio_taxons_csv",
    "ecoulement_observations_csv",
    "prelevements_chroniques_csv",
    "piezometry_stations_csv",
    "quality_groundwater_stations_csv",
    "quality_rivers_stations_csv",
    "temperature_stations_csv",
    "hydrometry_sites_csv",
    "hydrometry_stations_csv",
    "hydrobio_stations_csv",
    "ecoulement_stations_csv",
    "ecoulement_campagnes_csv",
    "prelevements_points_csv",
    "prelevements_ouvrages_csv",
]
