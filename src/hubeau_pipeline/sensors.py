"""
Dagster Sensors - Automated Pipeline Triggers

Sensors:
- bronze_to_silver_sensor: Triggers dbt after Bronze assets materialize
- freshness_alert_sensor: Alerts when source data is stale
"""

import os
from dagster import (
    asset_sensor,
    multi_asset_sensor,
    MultiAssetSensorDefinition,
    MultiAssetSensorEvaluationContext,
    AssetKey,
    RunRequest,
    SkipReason,
    DefaultSensorStatus,
    sensor,
    SensorEvaluationContext,
)
from datetime import datetime, timedelta
import logging

from .jobs import dbt_silver_gold_pipeline_job

logger = logging.getLogger(__name__)


def _env_true(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y"}


DEFAULT_SENSOR_STATUS = (
    DefaultSensorStatus.RUNNING
    if _env_true("DAGSTER_ENABLE_SENSORS", "false")
    else DefaultSensorStatus.STOPPED
)


# ==============================================================================
# BRONZE → SILVER AUTOMATION SENSOR
# ==============================================================================

@multi_asset_sensor(
    monitored_assets=[
        AssetKey("piezometry_chroniques_raw"),
        AssetKey("hydrometry_obs_elab_raw"),
    ],
    job=dbt_silver_gold_pipeline_job,
    minimum_interval_seconds=300,  # 5 minutes cooldown
    default_status=DEFAULT_SENSOR_STATUS,
    description="Triggers dbt Silver/Gold pipeline when Bronze data is updated",
)
def bronze_to_silver_sensor(context: MultiAssetSensorEvaluationContext):
    """
    Watches Bronze chroniques assets and triggers dbt transformation.
    
    Logic:
    - Waits for at least one of the monitored assets to materialize
    - Triggers dbt_silver_gold_pipeline_job
    - Uses run_key to deduplicate runs within same minute
    """
    asset_events = context.latest_materialization_records_by_key()
    
    # Check if any monitored asset has new materializations
    materialized_assets = []
    for asset_key, record in asset_events.items():
        if record is not None:
            materialized_assets.append(asset_key.to_user_string())
            context.advance_cursor({asset_key: record})
    
    if not materialized_assets:
        return SkipReason("No new Bronze materializations detected")
    
    # Generate unique run key (prevents duplicate runs in same minute)
    run_key = f"bronze_to_silver_{datetime.now().strftime('%Y%m%d_%H%M')}"
    
    logger.info(
        f"🔄 Bronze → Silver trigger: {', '.join(materialized_assets)} materialized. "
        f"Launching dbt pipeline..."
    )
    
    yield RunRequest(
        run_key=run_key,
        tags={
            "trigger": "sensor",
            "sensor_name": "bronze_to_silver_sensor",
            "triggered_by_assets": ",".join(materialized_assets),
        }
    )


# ==============================================================================
# PIEZOMETRY DAILY TRIGGER
# ==============================================================================

@asset_sensor(
    asset_key=AssetKey("piezometry_chroniques_daily_raw"),
    job=dbt_silver_gold_pipeline_job,
    minimum_interval_seconds=300,
    default_status=DEFAULT_SENSOR_STATUS,
    description="Triggers dbt after daily piezometry incremental load",
)
def piezometry_daily_to_silver_sensor(context, asset_event):
    """
    Watches daily piezometry asset and triggers dbt.
    Used for incremental daily pipeline automation.
    """
    run_key = f"piezo_daily_to_silver_{datetime.now().strftime('%Y%m%d_%H%M')}"
    
    logger.info("🔄 Daily Piezometry → Silver trigger: Launching dbt pipeline...")
    
    yield RunRequest(
        run_key=run_key,
        tags={
            "trigger": "sensor",
            "sensor_name": "piezometry_daily_to_silver_sensor",
            "mode": "daily_incremental",
        }
    )


# ==============================================================================
# HYDROMETRY DAILY TRIGGER
# ==============================================================================

@asset_sensor(
    asset_key=AssetKey("hydrometry_obs_daily_raw"),
    job=dbt_silver_gold_pipeline_job,
    minimum_interval_seconds=300,
    default_status=DEFAULT_SENSOR_STATUS,
    description="Triggers dbt after daily hydrometry incremental load",
)
def hydrometry_daily_to_silver_sensor(context, asset_event):
    """
    Watches daily hydrometry asset and triggers dbt.
    Used for incremental daily pipeline automation.
    """
    run_key = f"hydro_daily_to_silver_{datetime.now().strftime('%Y%m%d_%H%M')}"
    
    logger.info("🔄 Daily Hydrometry → Silver trigger: Launching dbt pipeline...")
    
    yield RunRequest(
        run_key=run_key,
        tags={
            "trigger": "sensor",
            "sensor_name": "hydrometry_daily_to_silver_sensor",
            "mode": "daily_incremental",
        }
    )


# ==============================================================================
# EXPORTS
# ==============================================================================

all_sensors = [
    bronze_to_silver_sensor,
    piezometry_daily_to_silver_sensor,
    hydrometry_daily_to_silver_sensor,
]
