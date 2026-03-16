"""
Dagster Jobs - Hub'Eau Pipeline

Domains:
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
from .dbt_jobs import (
    # Full pipeline (bootstrap/full refresh)
    dbt_silver_gold_pipeline_job,
    # Shared staging (run FIRST)
    dbt_shared_staging_job,
    # Domain-specific pipelines (can run in parallel after shared staging)
    dbt_piezo_pipeline_daily_job,
    dbt_piezo_pipeline_job,
    dbt_hydro_pipeline_daily_job,
    dbt_hydro_pipeline_job,
    # Shared dimensions (run LAST after domain pipelines)
    dbt_shared_dimensions_job,
    # Quality & docs
    dbt_test_job,
    dbt_freshness_job,
    dbt_quality_job,
    dbt_docs_job,
)

# Full Bootstrap Job (Complete population with partition iteration)
from .full_bootstrap_job import full_bootstrap_job

# Données de référence (BDLISA + Sandre) — à lancer avant full_bootstrap ou premier dbt run
from .reference_data_jobs import reference_data_bronze_job

# ML — SoftCLT Embeddings (uni + multi spaces)
from .ml_jobs import (
    ml_piezo_multi_train_job,
    ml_piezo_uni_train_job,
    ml_hydro_multi_train_job,
    ml_hydro_uni_train_job,
    ml_piezo_multi_embeddings_job,
    ml_piezo_uni_embeddings_job,
    ml_hydro_multi_embeddings_job,
    ml_hydro_uni_embeddings_job,
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
    # Jobs dbt - Domain-specific pipelines (parallel)
    dbt_piezo_pipeline_daily_job,
    dbt_piezo_pipeline_job,
    dbt_hydro_pipeline_daily_job,
    dbt_hydro_pipeline_job,
    # Jobs dbt - Shared dimensions (run LAST)
    dbt_shared_dimensions_job,
    # Jobs dbt - Quality & docs
    dbt_test_job,
    dbt_freshness_job,
    dbt_quality_job,
    dbt_docs_job,
    # Données de référence (BDLISA + Sandre)
    reference_data_bronze_job,
    # Full Bootstrap (complete population)
    full_bootstrap_job,
    # ML — SoftCLT Embeddings (uni + multi spaces)
    ml_piezo_multi_train_job,
    ml_piezo_uni_train_job,
    ml_hydro_multi_train_job,
    ml_hydro_uni_train_job,
    ml_piezo_multi_embeddings_job,
    ml_piezo_uni_embeddings_job,
    ml_hydro_multi_embeddings_job,
    ml_hydro_uni_embeddings_job,
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
    # Jobs dbt - Full pipeline (bootstrap/full refresh)
    "dbt_silver_gold_pipeline_job",
    # Jobs dbt - Shared staging (run FIRST)
    "dbt_shared_staging_job",
    # Jobs dbt - Domain-specific pipelines (parallel)
    "dbt_piezo_pipeline_daily_job",
    "dbt_piezo_pipeline_job",
    "dbt_hydro_pipeline_daily_job",
    "dbt_hydro_pipeline_job",
    # Jobs dbt - Shared dimensions (run LAST)
    "dbt_shared_dimensions_job",
    # Jobs dbt - Quality & docs
    "dbt_test_job",
    "dbt_freshness_job",
    "dbt_quality_job",
    "dbt_docs_job",
    # Données de référence (BDLISA + Sandre)
    "reference_data_bronze_job",
    # Full Bootstrap (complete population)
    "full_bootstrap_job",
    # ML — SoftCLT Embeddings (uni + multi spaces)
    "ml_piezo_multi_train_job",
    "ml_piezo_uni_train_job",
    "ml_hydro_multi_train_job",
    "ml_hydro_uni_train_job",
    "ml_piezo_multi_embeddings_job",
    "ml_piezo_uni_embeddings_job",
    "ml_hydro_multi_embeddings_job",
    "ml_hydro_uni_embeddings_job",
    # Collections
    "all_jobs",
]


