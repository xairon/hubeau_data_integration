"""
Jobs Dagster - Orchestration des pipelines d'ingestion
"""

# Jobs ancienne architecture (deprecated - à supprimer)
# from .bronze_ingestion import (
#     hubeau_hydrometry_job,
#     hubeau_piezometry_job,
#     hubeau_water_quality_surface_job,
#     hubeau_water_quality_groundwater_job,
#     hubeau_temperature_job,
#     hubeau_ecoulement_job,
#     hubeau_hydrobiology_job,
#     hubeau_prelevements_job
# )

# Jobs nouvelle architecture dlt (recommended)
from .dlt_jobs import (
    hydrobio_job,
    hydrometry_job,
    piezometry_job,
    quality_rivers_job,
    quality_groundwater_job,
    ecoulement_job,
    prelevements_job,
    temperature_job,
    sync_all_stations,
    sync_all_yearly_data,
)

# Jobs nouvelle architecture dlt COMPLETS (tous les attributs)
from .complete_data_jobs import (
    complete_data_ingestion_job,
    hydrometry_complete_job,
    piezometry_complete_job,
    quality_rivers_complete_job,
    quality_groundwater_complete_job,
    temperature_complete_job,
    ecoulement_complete_job,
    hydrobio_complete_job,
    prelevements_complete_job,
    reference_data_complete_job,
    temporal_data_complete_job,
)

# Jobs ancienne architecture (supprimés)
# old_jobs = []

# Jobs nouvelle architecture dlt
dlt_jobs = [
    hydrobio_job,
    hydrometry_job,
    piezometry_job,
    quality_rivers_job,
    quality_groundwater_job,
    ecoulement_job,
    prelevements_job,
    temperature_job,
    sync_all_stations,
    sync_all_yearly_data,
]

# Jobs nouvelle architecture dlt COMPLETS
dlt_complete_jobs = [
    complete_data_ingestion_job,
    hydrometry_complete_job,
    piezometry_complete_job,
    quality_rivers_complete_job,
    quality_groundwater_complete_job,
    temperature_complete_job,
    ecoulement_complete_job,
    hydrobio_complete_job,
    prelevements_complete_job,
    reference_data_complete_job,
    temporal_data_complete_job,
]

# ✅ NOUVELLE ARCHITECTURE: Utiliser les jobs dlt complets par défaut (TOUS les attributs)
all_jobs = dlt_complete_jobs

__all__ = [
    # Jobs nouvelle architecture dlt (recommended)
    "dlt_jobs",
    "hydrobio_job",
    "hydrometry_job",
    "piezometry_job",
    "quality_rivers_job",
    "quality_groundwater_job",
    "ecoulement_job",
    "prelevements_job",
    "temperature_job",
    "sync_all_stations",
    "sync_all_yearly_data",
    
    # Tous les jobs
    "all_jobs"
]
