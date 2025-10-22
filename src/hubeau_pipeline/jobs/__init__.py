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
    sync_all_observations_auto,
    sync_all_observations_by_year,
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
    sync_all_observations_auto,
    sync_all_observations_by_year,
]

# ✅ NOUVELLE ARCHITECTURE: Utiliser les jobs dlt par défaut
all_jobs = dlt_jobs

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
    "sync_all_observations_auto",
    "sync_all_observations_by_year",

    # Tous les jobs
    "all_jobs"
]
