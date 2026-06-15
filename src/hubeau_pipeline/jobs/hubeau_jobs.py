"""
Dagster Jobs - Hub'Eau Bronze Layer (Official Pattern)

Uses AssetSelection.groups() to select assets by group name.
This is compatible with @dlt_assets which generates asset names dynamically.

Reference: https://docs.dagster.io/api/libraries/dagster-dlt
"""

from dagster import define_asset_job, AssetSelection
from ..assets.bronze.dlt_assets import MODE_PARTITIONS


# ====================================
# JOBS PAR API - STATIONS (no partitions)
# ====================================

piezometry_stations_job = define_asset_job(
    name="piezometry_stations_bronze",
    description="Bronze: Piezometry stations (FULL load)",
    selection=AssetSelection.groups("piezometry_stations"),
    tags={"dagster/concurrency_key": "piezometry_stations_bronze"},
    hooks=set(),
)

hydrometry_stations_job = define_asset_job(
    name="hydrometry_stations_bronze",
    description="Bronze: Hydrometry sites + stations (FULL load)",
    selection=AssetSelection.groups("hydrometry_sites", "hydrometry_stations"),
    tags={"dagster/concurrency_key": "hydrometry_stations_bronze"},
    hooks=set(),
)


# ====================================
# JOBS PAR API - CHRONIQUES (partitioned)
# ====================================

piezometry_chroniques_job = define_asset_job(
    name="piezometry_chroniques_bronze",
    description="Bronze: Piezometry chroniques (partitioned: year)",
    selection=AssetSelection.groups("piezometry_chroniques"),
    partitions_def=MODE_PARTITIONS,
    tags={"dagster/concurrency_key": "piezometry_chroniques_bronze"},
    hooks=set(),
)

hydrometry_chroniques_job = define_asset_job(
    name="hydrometry_chroniques_bronze",
    description="Bronze: Hydrometry observations (partitioned: year)",
    selection=AssetSelection.groups("hydrometry_chroniques"),
    partitions_def=MODE_PARTITIONS,
    tags={"dagster/concurrency_key": "hydrometry_chroniques_bronze"},
    hooks=set(),
)


# ====================================
# JOBS DAILY (incremental)
# ====================================

daily_piezometry_bronze_job = define_asset_job(
    name="daily_piezometry_bronze",
    description="Bronze: Piezometry chroniques (last 7 days, incremental)",
    selection=AssetSelection.groups("piezometry_chroniques_daily"),
    tags={"dagster/concurrency_key": "daily_piezometry_bronze"},
    hooks=set(),
)

daily_hydrometry_bronze_job = define_asset_job(
    name="daily_hydrometry_bronze",
    description="Bronze: Hydrometry observations (last 7 days, incremental)",
    selection=AssetSelection.groups("hydrometry_chroniques_daily"),
    tags={"dagster/concurrency_key": "daily_hydrometry_bronze"},
    hooks=set(),
)

