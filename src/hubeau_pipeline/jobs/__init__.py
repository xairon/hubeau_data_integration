"""
Jobs Bronze Hub'Eau - Architecture moderne et claire
Jobs correspondant à la nouvelle structure des assets
"""

# Import des jobs Hub'Eau modernes
from .bronze_ingestion import (
    hubeau_bronze_job,
    hubeau_hydrology_job,
    hubeau_water_quality_job,
    hubeau_environment_job,
    hubeau_prelevements_job,
    hubeau_summary_job,
    bdlisa_bronze_job,
    sandre_bronze_job,
    all_jobs
)

# Jobs Hub'Eau modernes
hubeau_jobs = [
    hubeau_bronze_job,
    hubeau_hydrology_job,
    hubeau_water_quality_job,
    hubeau_environment_job,
    hubeau_prelevements_job,
    hubeau_summary_job
]

# Jobs externes (legacy)
external_jobs = [
    bdlisa_bronze_job,
    sandre_bronze_job
]

# Tous les jobs
bronze_jobs = all_jobs

__all__ = [
    "all_jobs",
    "bronze_jobs",
    "hubeau_jobs",
    "external_jobs",
    "hubeau_bronze_job",
    "hubeau_hydrology_job",
    "hubeau_water_quality_job",
    "hubeau_environment_job",
    "hubeau_prelevements_job",
    "hubeau_summary_job",
    "bdlisa_bronze_job",
    "sandre_bronze_job"
]