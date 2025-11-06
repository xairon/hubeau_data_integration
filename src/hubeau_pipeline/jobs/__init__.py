"""
Jobs Dagster - Bronze Layer

Jobs for Hub'Eau Bronze layer assets

ARCHITECTURE:
- *_stations jobs: FULL load (no partitions)
- *_chroniques jobs: Partitioned load (MODE_PARTITIONS: "full", "2024", etc.)

NOTE: Legacy CSV job removed - replaced by universal CSV ingestion asset
      See: assets/csv_universal.py (ingest_all_csvs_asset)
"""

# Legacy CSV job removed - now using universal CSV ingestion
# from .piezometers_job import piezometers_csv_job

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
    # Jobs CSV - removed legacy piezometers_csv_job
    # Now using universal CSV ingestion asset (no dedicated job needed)
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
    # Jobs CSV - removed (using universal CSV asset)
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
