"""
Jobs Dagster - Bronze Layer

Jobs for Hub'Eau Bronze layer assets

ARCHITECTURE:
- *_stations jobs: FULL load (no partitions)
- *_chroniques jobs: Partitioned load (MODE_PARTITIONS: "full", "2024", etc.)
"""

from .piezometers_job import piezometers_csv_job

from .hubeau_jobs import (
    # Jobs STATIONS (no partitions)
    piezometry_stations_job,
    quality_rivers_stations_job,
    quality_groundwater_stations_job,
    hydrometry_stations_job,
    temperature_stations_job,
    hydrobio_stations_job,
    ecoulement_stations_job,
    prelevements_stations_job,
    # Jobs CHRONIQUES (partitioned)
    piezometry_chroniques_job,
    quality_rivers_chroniques_job,
    quality_groundwater_chroniques_job,
    hydrometry_chroniques_job,
    temperature_chroniques_job,
    hydrobio_chroniques_job,
    ecoulement_chroniques_job,
    prelevements_chroniques_job,
    # Jobs GLOBAUX
    all_stations_job,
    all_chroniques_job,
)

all_jobs = [
    # Jobs CSV
    piezometers_csv_job,
    # Jobs STATIONS (no partitions)
    piezometry_stations_job,
    quality_rivers_stations_job,
    quality_groundwater_stations_job,
    hydrometry_stations_job,
    temperature_stations_job,
    hydrobio_stations_job,
    ecoulement_stations_job,
    prelevements_stations_job,
    # Jobs CHRONIQUES (partitioned)
    piezometry_chroniques_job,
    quality_rivers_chroniques_job,
    quality_groundwater_chroniques_job,
    hydrometry_chroniques_job,
    temperature_chroniques_job,
    hydrobio_chroniques_job,
    ecoulement_chroniques_job,
    prelevements_chroniques_job,
    # Jobs GLOBAUX
    all_stations_job,
    all_chroniques_job,
]

__all__ = [
    # Jobs CSV
    "piezometers_csv_job",
    # Jobs STATIONS (no partitions)
    "piezometry_stations_job",
    "quality_rivers_stations_job",
    "quality_groundwater_stations_job",
    "hydrometry_stations_job",
    "temperature_stations_job",
    "hydrobio_stations_job",
    "ecoulement_stations_job",
    "prelevements_stations_job",
    # Jobs CHRONIQUES (partitioned)
    "piezometry_chroniques_job",
    "quality_rivers_chroniques_job",
    "quality_groundwater_chroniques_job",
    "hydrometry_chroniques_job",
    "temperature_chroniques_job",
    "hydrobio_chroniques_job",
    "ecoulement_chroniques_job",
    "prelevements_chroniques_job",
    # Jobs GLOBAUX
    "all_stations_job",
    "all_chroniques_job",
    # Collections
    "all_jobs",
]
