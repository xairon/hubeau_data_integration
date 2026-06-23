"""
Dagster Jobs - Hub'Eau Pipeline

Domains:
- Piezometry (stations + chroniques)
- Hydrometry (sites + stations + observations)
- ERA5 (weather data)
"""

# Jobs dbt
from .dbt_jobs import (
    dbt_transform_job,            # ALL models, incremental (nightly via sensor + manual full refresh)
    dbt_quality_job,              # source freshness + dbt tests (nightly via sensor)
    dbt_docs_job,                 # docs generation (weekly schedule)
    station_current_index_job,    # IPS/SSFI nightly: fct_monthly_index + station_current_index (sensor)
    station_reference_stats_job,  # IPS baseline weekly (schedule)
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
    # --- Ingestion Bronze (manuelle / backfill) ---
    piezometry_stations_job,
    hydrometry_stations_job,
    piezometry_chroniques_job,      # partitioned
    hydrometry_chroniques_job,      # partitioned
    # --- Ingestion Bronze (daily, schedulée) ---
    daily_piezometry_bronze_job,
    daily_hydrometry_bronze_job,
    era5_meteo_job,                 # historique (manuel/bootstrap)
    era5_weekly_job,                # quotidien (schedule)
    # --- Transformation dbt ---
    dbt_transform_job,              # all models, nightly via sensor + manuel
    dbt_quality_job,                # tests + freshness, nightly via sensor
    dbt_docs_job,                   # docs, weekly schedule
    # --- Indices IPS/SSFI ---
    station_current_index_job,      # nightly via sensor
    station_reference_stats_job,    # baseline weekly via schedule
    # --- Référence (BDLISA / TME) ---
    reference_data_bronze_job,
    # --- Bootstrap complet (one-shot) ---
    full_bootstrap_job,
    # --- Qualité : détection de trous d'ingestion (weekly schedule) ---
    data_completeness_job,
]

__all__ = [
    "piezometry_stations_job",
    "hydrometry_stations_job",
    "piezometry_chroniques_job",
    "hydrometry_chroniques_job",
    "daily_piezometry_bronze_job",
    "daily_hydrometry_bronze_job",
    "era5_meteo_job",
    "era5_weekly_job",
    "dbt_transform_job",
    "dbt_quality_job",
    "dbt_docs_job",
    "station_current_index_job",
    "station_reference_stats_job",
    "reference_data_bronze_job",
    "full_bootstrap_job",
    "data_completeness_job",
    "all_jobs",
]
