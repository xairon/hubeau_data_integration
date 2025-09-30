"""
Assets Bronze Hub'Eau - Architecture moderne et claire
httpx + tenacity + pydantic pour une ingestion robuste et performante
"""

# Assets Hub'Eau Bronze
from .hubeau_assets import (
    hubeau_hydrometry_bronze,
    hubeau_piezometry_bronze,
    hubeau_water_quality_surface_bronze,
    hubeau_water_quality_groundwater_bronze,
    hubeau_temperature_bronze,
    hubeau_onde_bronze,
    hubeau_hydrobiology_bronze,
    hubeau_prelevements_bronze,
    hubeau_ingestion_summary
)

# Sources externes (référentiels géographiques et thesaurus)
from .legacy.bdlisa_real_ingestion import bdlisa_geographic_bronze_real
from .legacy.sandre_real_ingestion import sandre_thesaurus_bronze_real

# Assets de production Hub'Eau
hubeau_bronze_assets = [
    hubeau_hydrometry_bronze,
    hubeau_piezometry_bronze,
    hubeau_water_quality_surface_bronze,
    hubeau_water_quality_groundwater_bronze,
    hubeau_temperature_bronze,
    hubeau_onde_bronze,
    hubeau_hydrobiology_bronze,
    hubeau_prelevements_bronze,
    hubeau_ingestion_summary
]

# Assets externes (référentiels complémentaires)
external_bronze_assets = [
    bdlisa_geographic_bronze_real,  # BDLISA : Formations géologiques aquifères
    sandre_thesaurus_bronze_real     # Sandre : Nomenclatures et référentiels
]

# Tous les assets bronze
all_bronze_assets = hubeau_bronze_assets + external_bronze_assets

__all__ = [
    # Assets Hub'Eau Bronze
    "hubeau_bronze_assets",
    "hubeau_hydrometry_bronze",
    "hubeau_piezometry_bronze",
    "hubeau_water_quality_surface_bronze",
    "hubeau_water_quality_groundwater_bronze",
    "hubeau_temperature_bronze",
    "hubeau_onde_bronze",
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