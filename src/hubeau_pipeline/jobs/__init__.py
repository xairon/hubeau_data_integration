"""
Dagster Jobs - Hub'Eau Bronze Layer + Reference Data

Stations jobs: FULL load (no partitions)
Chroniques jobs: Partitioned load (full, 2020-2025)
Reference jobs: SANDRE & BD-LISA data
"""

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

# Jobs REFERENCE DATA (SANDRE & BD-LISA)
from .reference_jobs import (
    sandre_full_load_job,
    bdlisa_spatial_load_job,
    reference_data_full_load_job,
)

# Jobs ERA5
from .era5_jobs import (
    era5_meteo_job,
)

all_jobs = [
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
    # Jobs REFERENCE DATA
    sandre_full_load_job,
    bdlisa_spatial_load_job,
    reference_data_full_load_job,
    # Jobs ERA5
    era5_meteo_job,
]

__all__ = [
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
    # Jobs REFERENCE DATA
    "sandre_full_load_job",
    "bdlisa_spatial_load_job",
    "reference_data_full_load_job",
    # Jobs ERA5
    "era5_meteo_job",
    # Collections
    "all_jobs",
]
