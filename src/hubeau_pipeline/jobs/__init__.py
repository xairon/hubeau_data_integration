"""
Jobs Bronze Hub'Eau - Architecture simple : 1 job par API
"""

# Import de tous les jobs
from .bronze_ingestion import (
    all_jobs,
    bdlisa_bronze_job,
    external_jobs,
    hubeau_hydrobiology_job,
    hubeau_hydrometry_job,
    hubeau_jobs,
    hubeau_ecoulement_job,
    hubeau_piezometry_job,
    hubeau_prelevements_job,
    hubeau_hydrobio_taxons_job,
    hubeau_temperature_job,
    hubeau_water_quality_groundwater_job,
    hubeau_water_quality_surface_job,
    sandre_bronze_job,
)

# Alias pour compatibilité
bronze_jobs = all_jobs

__all__ = [
    "all_jobs",
    "bronze_jobs",
    "hubeau_jobs",
    "external_jobs",
    # Jobs Hub'Eau (1 par API)
    "hubeau_hydrometry_job",
    "hubeau_piezometry_job",
    "hubeau_temperature_job",
    "hubeau_water_quality_surface_job",
    "hubeau_water_quality_groundwater_job",
    "hubeau_ecoulement_job",
    "hubeau_hydrobiology_job",
    "hubeau_hydrobio_taxons_job",
    "hubeau_prelevements_job",
    # Jobs externes
    "bdlisa_bronze_job",
    "sandre_bronze_job",
]
