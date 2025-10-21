"""
Raw assets Hub'Eau - Raw data ingestion layer

This layer contains raw data from Hub'Eau API with minimal transformation:
- Direct API ingestion
- Data validation (pydantic)
- Resilient HTTP client (httpx + tenacity)

Standard: Modern Data Stack - Raw layer conventions.
"""

# Assets Hub'Eau Raw (ancienne architecture - deprecated)
# from .hubeau_assets import (
#     hubeau_hydrometry_bronze,
#     hubeau_piezometry_bronze,
#     hubeau_water_quality_surface_bronze,
#     hubeau_water_quality_groundwater_bronze,
#     hubeau_temperature_bronze,
#     hubeau_ecoulement_bronze,
#     hubeau_hydrobiology_bronze,
#     hubeau_prelevements_bronze,
#     hubeau_ingestion_summary
# )

# Assets Hub'Eau dlt (nouvelle architecture - recommended)
from .dlt_assets import (
    hydrobio_taxons,
    hydrobio_indices,
    hydrometry_obs_elab,
    piezometry_chroniques,
    piezometry_chroniques_historical,
    quality_rivers_analyses,
    quality_rivers_operations,
    quality_rivers_conditions,
    quality_groundwater_analyses,
    ecoulement_observations,
    prelevements_chroniques,
    temperature_chroniques,
    temperature_stations_reference,
    # Nouveaux assets de stations de référence
    hydrometry_stations_reference,
    hydrometry_sites_reference,
    piezometry_stations_reference,
    quality_rivers_stations_reference,
    quality_groundwater_stations_reference,
    ecoulement_stations_reference,
    hydrobio_stations_reference,
    prelevements_ouvrages_reference,
    prelevements_points_reference,
)

# Sources externes (référentiels géographiques et thesaurus) - SUPPRIMÉS
# from .legacy.bdlisa_real_ingestion import bdlisa_geographic_bronze_real
# from .legacy.sandre_real_ingestion import sandre_thesaurus_bronze_real

# Assets de production Hub'Eau (ancienne architecture - supprimés)
# hubeau_raw_assets_old = []

# Assets de production Hub'Eau (nouvelle architecture dlt - recommandé)
hubeau_raw_assets = [
    # Assets de stations de référence (pas de partition)
    hydrometry_stations_reference,
    hydrometry_sites_reference,
    piezometry_stations_reference,
    quality_rivers_stations_reference,
    quality_groundwater_stations_reference,
    ecoulement_stations_reference,
    hydrobio_stations_reference,
    prelevements_ouvrages_reference,
    prelevements_points_reference,
    temperature_stations_reference,

    # Assets d'observations/analyses (avec partitions annuelles)
    hydrobio_taxons,
    hydrobio_indices,
    hydrometry_obs_elab,
    piezometry_chroniques,
    piezometry_chroniques_historical,
    quality_rivers_analyses,
    quality_rivers_operations,
    quality_rivers_conditions,
    quality_groundwater_analyses,
    ecoulement_observations,
    prelevements_chroniques,
    temperature_chroniques,
]

# Assets externes (référentiels complémentaires) - SUPPRIMÉS
# external_raw_assets = [
#     bdlisa_geographic_raw_real,  # BDLISA : Formations géologiques aquifères
#     sandre_thesaurus_raw_real     # Sandre : Nomenclatures et référentiels
# ]

# ✅ NOUVELLE ARCHITECTURE: Utiliser les assets dlt par défaut
all_raw_assets = hubeau_raw_assets

__all__ = [
    # Assets Hub'Eau dlt (nouvelle architecture)
    "hubeau_raw_assets",
    "hydrobio_taxons",
    "hydrobio_indices",
    "hydrometry_obs_elab",
    "piezometry_chroniques",
    "piezometry_chroniques_historical",
    "quality_rivers_analyses",
    "quality_rivers_operations",
    "quality_rivers_conditions",
    "quality_groundwater_analyses",
    "ecoulement_observations",
    "prelevements_chroniques",
    "temperature_chroniques",
    "temperature_stations_reference",
    # Nouveaux assets de stations de référence
    "hydrometry_stations_reference",
    "hydrometry_sites_reference",
    "piezometry_stations_reference",
    "quality_rivers_stations_reference",
    "quality_groundwater_stations_reference",
    "ecoulement_stations_reference",
    "hydrobio_stations_reference",
    "prelevements_ouvrages_reference",
    "prelevements_points_reference",
    # Assets de diagnostic
    "diagnose_piezometry_missing_stations",

    # Tous les assets
    "all_raw_assets"
]
