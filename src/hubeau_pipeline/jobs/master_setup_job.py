"""
Master Setup Job - One-Click Full Database Population

This job orchestrates the complete initial data load:
1. All Bronze stations (Hub'Eau + ERA5 recent)
2. dbt Silver/Gold transformations

Note: Partitioned chroniques (historical data) must be run separately
due to their size and partitioning requirements.
"""

from dagster import (
    define_asset_job,
    AssetSelection,
    Definitions,
    job,
    op,
    In,
    Nothing,
    graph,
    OpExecutionContext,
)
from ..hooks import log_failure_hook, slack_failure_hook, email_failure_hook

# FAILURE_HOOKS removed as per request


# ==============================================================================
# MASTER SETUP JOB - Non-partitioned assets only
# ==============================================================================

# This job loads all NON-PARTITIONED assets in the correct order
# Partitioned assets (chroniques, ERA5 historical) need separate runs

master_bronze_stations_job = define_asset_job(
    name="master_bronze_stations",
    description=(
        "🚀 MASTER SETUP STEP 1/3: Load ALL station metadata (non-partitioned). "
        "This includes piezometry stations, hydrometry sites, and hydrometry stations."
    ),
    selection=AssetSelection.groups(
        "piezometry_stations",
        "hydrometry_sites", 
        "hydrometry_stations",
    ),
    tags={"dagster/priority": "1", "dagster/concurrency_key": "master_setup"},
    hooks=set(),
)


master_bronze_recent_job = define_asset_job(
    name="master_bronze_recent",
    description=(
        "🚀 MASTER SETUP STEP 2/3: Load recent data (last 7-60 days). "
        "Includes daily piezometry, daily hydrometry, and ERA5 weekly update."
    ),
    selection=AssetSelection.groups(
        "piezometry_chroniques_daily",
        "hydrometry_chroniques_daily",
        "era5_weekly",
    ),
    tags={"dagster/priority": "2", "dagster/concurrency_key": "master_setup"},
    hooks=set(),
)


master_dbt_transform_job = define_asset_job(
    name="master_dbt_transform",
    description=(
        "🚀 MASTER SETUP STEP 3/3: Run dbt transformations (Silver/Gold). "
        "Transforms Bronze data into analytical models."
    ),
    selection=AssetSelection.groups("dbt"),
    tags={"dagster/priority": "3", "dagster/concurrency_key": "master_setup"},
    hooks=set(),
)


# ==============================================================================
# FULL REFRESH JOB (for partitioned data - use Dagster UI)
# ==============================================================================
# Note: For full historical load, use Dagster UI to:
# 1. Run "all_chroniques_bronze" with ALL partitions selected
# 2. Run "era5_historical_load" with desired year partitions
# These are too large to bundle in a single job.
