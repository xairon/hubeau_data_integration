"""
Assets Bronze Hub'Eau - Architecture moderne et claire
httpx + tenacity + pydantic pour une ingestion robuste et performante
"""

# Assets Hub'Eau Bronze (ancienne architecture - deprecated)
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
    hydrometry_observations,
    piezometry_chroniques,
    quality_rivers_analyses,
    quality_groundwater_analyses,
    ecoulement_observations,
    prelevements_chroniques,
    temperature_chroniques,
    temperature_stations_reference,
    # Nouveaux assets de stations de référence
    hydrometry_stations_reference,
    piezometry_stations_reference,
    quality_rivers_stations_reference,
    quality_groundwater_stations_reference,
    ecoulement_stations_reference,
    hydrobio_stations_reference,
    prelevements_ouvrages_reference,
    prelevements_points_reference,
)

# Sources externes (référentiels géographiques et thesaurus)
from .legacy.bdlisa_real_ingestion import bdlisa_geographic_bronze_real
from .legacy.sandre_real_ingestion import sandre_thesaurus_bronze_real

# Assets de production Hub'Eau (ancienne architecture - supprimés)
# hubeau_bronze_assets_old = []

# Assets de production Hub'Eau (nouvelle architecture dlt - recommandé)
hubeau_bronze_assets_dlt = [
    # Assets de stations de référence (pas de partition)
    hydrometry_stations_reference,
    piezometry_stations_reference,
    quality_rivers_stations_reference,
    quality_groundwater_stations_reference,
    ecoulement_stations_reference,
    hydrobio_stations_reference,
    prelevements_ouvrages_reference,
    prelevements_points_reference,
    temperature_stations_reference,
    
    # Assets d'observations/analyses (avec partitions)
    hydrobio_taxons,
    hydrobio_indices,
    hydrometry_observations,
    piezometry_chroniques,
    quality_rivers_analyses,
    quality_groundwater_analyses,
    ecoulement_observations,
    prelevements_chroniques,
    temperature_chroniques,
]

# Assets externes (référentiels complémentaires)
external_bronze_assets = [
    bdlisa_geographic_bronze_real,  # BDLISA : Formations géologiques aquifères
    sandre_thesaurus_bronze_real     # Sandre : Nomenclatures et référentiels
]

# ✅ NOUVELLE ARCHITECTURE: Utiliser les assets dlt par défaut
all_bronze_assets = hubeau_bronze_assets_dlt + external_bronze_assets

__all__ = [
    # Assets Hub'Eau dlt (nouvelle architecture)
    "hubeau_bronze_assets_dlt",
    "hydrobio_taxons",
    "hydrobio_indices",
    "hydrometry_observations",
    "piezometry_chroniques",
    "quality_rivers_analyses",
    "quality_groundwater_analyses",
    "ecoulement_observations",
    "prelevements_chroniques",
    "temperature_chroniques",
    "temperature_stations_reference",
    # Nouveaux assets de stations de référence
    "hydrometry_stations_reference",
    "piezometry_stations_reference",
    "quality_rivers_stations_reference",
    "quality_groundwater_stations_reference",
    "ecoulement_stations_reference",
    "hydrobio_stations_reference",
    "prelevements_ouvrages_reference",
    "prelevements_points_reference",
    
    # Assets Hub'Eau Bronze (ancienne architecture - deprecated)
    "hubeau_bronze_assets_old",
    "hubeau_hydrometry_bronze",
    "hubeau_piezometry_bronze",
    "hubeau_water_quality_surface_bronze",
    "hubeau_water_quality_groundwater_bronze",
    "hubeau_temperature_bronze",
    "hubeau_ecoulement_bronze",
    "hubeau_hydrobiology_bronze",
    "hubeau_prelevements_bronze",
    "hubeau_ingestion_summary",
    
    # Assets externes (référentiels)
    "external_bronze_assets",
    "bdlisa_geographic_bronze_real",
    "sandre_thesaurus_bronze_real",
    
    # Tous les assets
    "all_bronze_assets"
]
