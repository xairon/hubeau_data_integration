"""
Jobs Dagster - Orchestration des pipelines d'ingestion
"""

# Jobs ancienne architecture (deprecated)
from .bronze_ingestion import (
    hubeau_hydrometry_job,
    hubeau_piezometry_job,
    hubeau_water_quality_surface_job,
    hubeau_water_quality_groundwater_job,
    hubeau_temperature_job,
    hubeau_ecoulement_job,
    hubeau_hydrobiology_job,
    hubeau_prelevements_job
)

# Jobs nouvelle architecture dlt (recommended)
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from dagster.jobs import (
    hydrobio_job,
    hydrometry_job,
    piezometry_job,
    quality_job,
    ecoulement_job,
    prelevements_job,
    temperature_job,
    sync_hubeau_daily,
    sync_hubeau_realtime,
    sync_hubeau_quality,
)

# Jobs ancienne architecture
old_jobs = [
    hubeau_hydrometry_job,
    hubeau_piezometry_job,
    hubeau_water_quality_surface_job,
    hubeau_water_quality_groundwater_job,
    hubeau_temperature_job,
    hubeau_ecoulement_job,
    hubeau_hydrobiology_job,
    hubeau_prelevements_job
]

# Jobs nouvelle architecture dlt
dlt_jobs = [
    hydrobio_job,
    hydrometry_job,
    piezometry_job,
    quality_job,
    ecoulement_job,
    prelevements_job,
    temperature_job,
    sync_hubeau_daily,
    sync_hubeau_realtime,
    sync_hubeau_quality,
]

# ✅ NOUVELLE ARCHITECTURE: Utiliser les jobs dlt par défaut
all_jobs = dlt_jobs

__all__ = [
    # Jobs nouvelle architecture dlt (recommended)
    "dlt_jobs",
    "hydrobio_job",
    "hydrometry_job",
    "piezometry_job",
    "quality_job",
    "ecoulement_job",
    "prelevements_job",
    "temperature_job",
    "sync_hubeau_daily",
    "sync_hubeau_realtime",
    "sync_hubeau_quality",
    
    # Jobs ancienne architecture (deprecated)
    "old_jobs",
    "hubeau_hydrometry_job",
    "hubeau_piezometry_job",
    "hubeau_water_quality_surface_job",
    "hubeau_water_quality_groundwater_job",
    "hubeau_temperature_job",
    "hubeau_ecoulement_job",
    "hubeau_hydrobiology_job",
    "hubeau_prelevements_job",
    
    # Tous les jobs
    "all_jobs"
]
