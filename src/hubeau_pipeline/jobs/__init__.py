"""
Dagster Jobs - Hub'Eau Pipeline

Domains:
- Piezometry (stations + chroniques)
- Hydrometry (sites + stations + observations)
- ERA5 (weather data)
"""

# Jobs dbt
from .dbt_jobs import (
    # Daily transform (single job: both domains + shared dims, sensor-driven)
    dbt_daily_transform_job,
    dbt_docs_job,
    dbt_freshness_job,
    dbt_quality_job,
    # Shared staging (run FIRST)
    dbt_shared_staging_job,
    # Full pipeline (bootstrap/full refresh)
    dbt_silver_gold_pipeline_job,
    # Quality & docs
    dbt_test_job,
    # Indices nocturnes (sensor) : fct_monthly_index + station_current_index
    station_current_index_job,
    # Indice baseline (schedule hebdo) : station_reference_stats
    station_reference_stats_job,
)

# Jobs ERA5
from .era5_jobs import (
    era5_meteo_job,
    era5_weekly_job,
)

# Full Bootstrap Job (Complete population with partition iteration)
from .completeness_job import data_completeness_job
from .full_bootstrap_job import full_bootstrap_job
from .hubeau_jobs import (
    daily_hydrometry_bronze_job,
    # Jobs DAILY (incremental)
    daily_piezometry_bronze_job,
    hydrometry_chroniques_job,
    hydrometry_stations_job,
    # Jobs CHRONIQUES (partitioned)
    piezometry_chroniques_job,
    # Jobs STATIONS (no partitions)
    piezometry_stations_job,
)

# Données de référence (BDLISA / TME) — à lancer avant full_bootstrap ou premier dbt run
from .reference_data_jobs import reference_data_bronze_job

all_jobs = [
    # Jobs STATIONS (no partitions)
    piezometry_stations_job,
    hydrometry_stations_job,
    # Jobs CHRONIQUES (partitioned)
    piezometry_chroniques_job,
    hydrometry_chroniques_job,
    # Jobs DAILY (incremental)
    daily_piezometry_bronze_job,
    daily_hydrometry_bronze_job,
    # Jobs ERA5
    era5_meteo_job,
    era5_weekly_job,
    # Jobs dbt - Full pipeline (bootstrap/full refresh)
    dbt_silver_gold_pipeline_job,
    # Jobs dbt - Shared staging (run FIRST)
    dbt_shared_staging_job,
    # Jobs dbt - Daily transform (single job, sensor-driven)
    dbt_daily_transform_job,
    # Jobs indices - nocturne (monthly + current) + baseline hebdo
    station_current_index_job,
    station_reference_stats_job,
    # Jobs dbt - Quality & docs
    dbt_test_job,
    dbt_freshness_job,
    dbt_quality_job,
    dbt_docs_job,
    # Données de référence (BDLISA / TME)
    reference_data_bronze_job,
    # Full Bootstrap (complete population)
    full_bootstrap_job,
    # Qualité - détection de trous d'ingestion
    data_completeness_job,
]

__all__ = [
    # Jobs STATIONS (no partitions)
    "piezometry_stations_job",
    "hydrometry_stations_job",
    # Jobs CHRONIQUES (partitioned)
    "piezometry_chroniques_job",
    "hydrometry_chroniques_job",
    # Jobs DAILY (incremental)
    "daily_piezometry_bronze_job",
    "daily_hydrometry_bronze_job",
    # Jobs ERA5
    "era5_meteo_job",
    "era5_weekly_job",
    # Jobs dbt - Full pipeline (bootstrap/full refresh)
    "dbt_silver_gold_pipeline_job",
    # Jobs dbt - Shared staging (run FIRST)
    "dbt_shared_staging_job",
    # Jobs dbt - Daily transform (single job, sensor-driven)
    "dbt_daily_transform_job",
    # Jobs indices
    "station_current_index_job",
    "station_reference_stats_job",
    # Jobs dbt - Quality & docs
    "dbt_test_job",
    "dbt_freshness_job",
    "dbt_quality_job",
    "dbt_docs_job",
    # Données de référence (BDLISA / TME)
    "reference_data_bronze_job",
    # Full Bootstrap (complete population)
    "full_bootstrap_job",
    # Qualité - détection de trous d'ingestion
    "data_completeness_job",
    # Collections
    "all_jobs",
]


