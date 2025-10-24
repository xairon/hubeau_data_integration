"""
Assets Bronze Layer - CSV Ingestion Hub'Eau

Ce layer contient les assets d'ingestion directe CSV depuis l'API Hub'Eau.
Architecture multi-mode (FULL, YEAR, INCREMENTAL) avec support DLT.

22 endpoints Hub'Eau :
- 11 chroniques/observations (avec filtres date)
- 11 stations/référentiels (sans filtres date)
"""

from .csv_assets import (
    # Chroniques/Observations (avec filtres date + slicing pour piezometry)
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

# Liste de tous les assets CSV
all_csv_assets = [
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

__all__ = [
    "all_csv_assets",
    # Chroniques/Observations
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
    # Stations/Référentiels
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
