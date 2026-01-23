"""
Dagster Jobs - Hub'Eau Bronze Layer + ERA5 + Aggregation (Legacy)

KEPT DOMAINS:
- Piezometry (stations + chroniques)
- Hydrometry (sites + stations + observations)
- ERA5 (weather data)
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
    # Jobs DAILY (incremental)
    daily_piezometry_bronze_job,
    daily_hydrometry_bronze_job,
)

# Jobs ERA5
from .era5_jobs import (
    era5_meteo_job,
    era5_weekly_job,
)

# Jobs dbt
from .dbt_jobs import dbt_silver_gold_pipeline_job

# Master Setup Jobs (One-Click)
from .master_setup_job import (
    master_bronze_stations_job,
    master_bronze_recent_job,
    master_dbt_transform_job,
)

# Full Bootstrap Job (Complete population with partition iteration)
from .full_bootstrap_job import full_bootstrap_job

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
    # Jobs DAILY (incremental)
    daily_piezometry_bronze_job,
    daily_hydrometry_bronze_job,
    # Jobs ERA5
    era5_meteo_job,
    era5_weekly_job,
    # Jobs dbt (Silver/Gold layers)
    dbt_silver_gold_pipeline_job,
    # Master Setup Jobs
    master_bronze_stations_job,
    master_bronze_recent_job,
    master_dbt_transform_job,
    # Full Bootstrap (complete population)
    full_bootstrap_job,
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
    # Jobs DAILY (incremental)
    "daily_piezometry_bronze_job",
    "daily_hydrometry_bronze_job",
    # Jobs ERA5
    "era5_meteo_job",
    "era5_weekly_job",
    # Jobs dbt (Silver/Gold layers)
    "dbt_silver_gold_pipeline_job",
    # Master Setup Jobs
    "master_bronze_stations_job",
    "master_bronze_recent_job",
    "master_dbt_transform_job",
    # Full Bootstrap (complete population)
    "full_bootstrap_job",
    # Collections
    "all_jobs",
]


