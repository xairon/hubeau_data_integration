"""
Assets Hub'Eau - Ingestion directe PostgreSQL avec type mappings

Structure:
- hubeau_assets.py      : Ingestion des données Hub'Eau avec types corrects dès la création
- monitoring/           : Monitoring qualité données

NOTE: Schema optimization assets supprimés - types mappés dès la création via hubeau_type_mappings.py
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

# Tous les assets du pipeline (schema optimization assets supprimés)
all_assets = all_hubeau_assets + all_monitoring_assets

__all__ = [
    "all_assets",
    "all_hubeau_assets",
    "all_monitoring_assets",
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
