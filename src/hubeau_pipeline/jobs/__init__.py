"""
Dagster Jobs - Hub'Eau Bronze Layer + ERA5 + Aggregation

KEPT DOMAINS:
- Piezometry (stations + chroniques)
- Hydrometry (sites + stations + observations)
- ERA5 (weather data)
- Aggregation (ERA5 + Piezometry combined)
"""

from .hubeau_jobs import (
    # Jobs STATIONS (no partitions)
    piezometry_stations_job,
    hydrometry_stations_job,
    # Jobs CHRONIQUES (partitioned)
    piezometry_chroniques_job,
    hydrometry_chroniques_job,
    # Jobs GLOBAUX
    all_stations_job,
    all_chroniques_job,
)

# Jobs ERA5
from .era5_jobs import (
    era5_meteo_job,
    era5_timeseries_job,
)

# Jobs Aggregation
from .aggregation_jobs import (
    aggregation_job,
)

all_jobs = [
    # Jobs STATIONS (no partitions)
    piezometry_stations_job,
    hydrometry_stations_job,
    # Jobs CHRONIQUES (partitioned)
    piezometry_chroniques_job,
    hydrometry_chroniques_job,
    # Jobs GLOBAUX
    all_stations_job,
    all_chroniques_job,
    # Jobs ERA5
    era5_meteo_job,
    era5_timeseries_job,
    # Jobs Aggregation
    aggregation_job,
]

__all__ = [
    # Jobs STATIONS (no partitions)
    "piezometry_stations_job",
    "hydrometry_stations_job",
    # Jobs CHRONIQUES (partitioned)
    "piezometry_chroniques_job",
    "hydrometry_chroniques_job",
    # Jobs GLOBAUX
    "all_stations_job",
    "all_chroniques_job",
    # Jobs ERA5
    "era5_meteo_job",
    "era5_timeseries_job",
    # Jobs Aggregation
    "aggregation_job",
    # Collections
    "all_jobs",
]
