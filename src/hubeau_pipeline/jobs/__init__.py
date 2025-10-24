"""
Jobs Dagster - Orchestration des pipelines Hub'Eau

Un job par API pour faciliter l'exécution sélective
"""

from .hubeau_jobs import (
    # Jobs par API
    piezometry_job,
    quality_rivers_job,
    quality_groundwater_job,
    hydrometry_job,
    temperature_job,
    hydrobio_job,
    ecoulement_job,
    prelevements_job,
    # Jobs globaux
    all_stations_job,
    all_chroniques_job,
    all_hubeau_job,
)

all_jobs = [
    piezometry_job,
    quality_rivers_job,
    quality_groundwater_job,
    hydrometry_job,
    temperature_job,
    hydrobio_job,
    ecoulement_job,
    prelevements_job,
    all_stations_job,
    all_chroniques_job,
    all_hubeau_job,
]

__all__ = [
    # Jobs par API
    "piezometry_job",
    "quality_rivers_job",
    "quality_groundwater_job",
    "hydrometry_job",
    "temperature_job",
    "hydrobio_job",
    "ecoulement_job",
    "prelevements_job",
    # Jobs globaux
    "all_stations_job",
    "all_chroniques_job",
    "all_hubeau_job",
    # Collections
    "all_jobs",
]
