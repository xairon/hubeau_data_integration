"""
Jobs Dagster - Orchestration des pipelines d'ingestion
Architecture: Un job par API (comme l'ancien système JSON)
"""

# Jobs CSV - Un job par API
from .csv_jobs import (
    # Jobs par API
    piezometry_csv_job,
    quality_rivers_csv_job,
    quality_groundwater_csv_job,
    hydrometry_csv_job,
    temperature_csv_job,
    hydrobio_csv_job,
    ecoulement_csv_job,
    prelevements_csv_job,
    # Jobs globaux
    all_stations_csv_job,
    all_chroniques_csv_job,
    all_hubeau_csv_job,
)

# Jobs CSV par API
csv_jobs = [
    piezometry_csv_job,
    quality_rivers_csv_job,
    quality_groundwater_csv_job,
    hydrometry_csv_job,
    temperature_csv_job,
    hydrobio_csv_job,
    ecoulement_csv_job,
    prelevements_csv_job,
    all_stations_csv_job,
    all_chroniques_csv_job,
    all_hubeau_csv_job,
]

# ✅ ARCHITECTURE CSV: Un job par API (comme l'ancien système)
all_jobs = csv_jobs

__all__ = [
    # Jobs par API
    "piezometry_csv_job",
    "quality_rivers_csv_job",
    "quality_groundwater_csv_job",
    "hydrometry_csv_job",
    "temperature_csv_job",
    "hydrobio_csv_job",
    "ecoulement_csv_job",
    "prelevements_csv_job",
    # Jobs globaux
    "all_stations_csv_job",
    "all_chroniques_csv_job",
    "all_hubeau_csv_job",
    # Collections
    "csv_jobs",
    "all_jobs"
]
