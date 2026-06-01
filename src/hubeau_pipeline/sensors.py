"""
Dagster Sensors - Event-Driven Pipeline Orchestration

Architecture:
  Bronze materializes → sensor detects → launches sequential chain:
    1. Shared staging (ERA5) — MUST complete first
    2. Daily transform — full Silver→Gold for both domains + shared dimensions,
       in ONE dbt job (dbt resolves intra-job ordering via the ref() DAG)
    3. Current index — compute per-station IPS/SSFI after daily transform

Each step only starts after its prerequisite completes successfully.

History: a previous 3-step design fanned out to parallel piezo+hydro jobs and
rejoined via a cursor-tracking RunStatusSensor. RunStatusSensorContext has no
cursor, so the rejoin (step 3, dimensions) crashed on every tick and silently
stopped refreshing monthly/yearly aggregates, station dimensions and dim_date.
Since runs are serialized globally (max_concurrent_runs=1) the fan-out gave no
speedup anyway, so steps 2+3 were collapsed into a single robust job.
"""

import logging
from datetime import date

from dagster import (
    AssetKey,
    DagsterRunStatus,
    DefaultSensorStatus,
    MultiAssetSensorEvaluationContext,
    RunRequest,
    RunStatusSensorContext,
    SkipReason,
    multi_asset_sensor,
    run_status_sensor,
)

from .jobs import (
    dbt_daily_transform_job,
    dbt_shared_staging_job,
    station_current_index_job,
)
from .utils import env_true

logger = logging.getLogger(__name__)


DEFAULT_SENSOR_STATUS = (
    DefaultSensorStatus.RUNNING
    if env_true("DAGSTER_ENABLE_SENSORS", "false")
    else DefaultSensorStatus.STOPPED
)


# ==============================================================================
# STEP 1: BRONZE → SHARED STAGING
# ==============================================================================

@multi_asset_sensor(
    monitored_assets=[
        AssetKey("piezometry_chroniques_daily_raw"),
        AssetKey("hydrometry_obs_daily_raw"),
    ],
    job=dbt_shared_staging_job,
    minimum_interval_seconds=300,  # 5 min cooldown
    default_status=DEFAULT_SENSOR_STATUS,
    description="Step 1/3: Bronze materializes → launch shared ERA5 staging",
)
def bronze_to_shared_staging_sensor(context: MultiAssetSensorEvaluationContext):
    """
    Watches Bronze chroniques and triggers shared staging (ERA5).
    This is the entry point of the event-driven chain.
    """
    asset_events = context.latest_materialization_records_by_key()

    materialized_assets = []
    max_storage_id = 0
    for asset_key, record in asset_events.items():
        if record is not None:
            materialized_assets.append(asset_key.to_user_string())
            max_storage_id = max(max_storage_id, record.storage_id)

    # Always advance all cursors to prevent trailing_unconsumed_events overflow.
    # Must be called whether we yield a RunRequest or skip.
    context.advance_all_cursors()

    if not materialized_assets:
        return SkipReason("No new Bronze materializations detected")

    # Use date-based run_key to deduplicate: piezo and hydro materialize at
    # different times but should only trigger ONE shared staging per day.
    run_key = f"shared_staging_{date.today().isoformat()}"

    logger.info(
        f"Step 1/3: Bronze materialized ({', '.join(materialized_assets)}). "
        f"Launching shared staging..."
    )

    yield RunRequest(
        run_key=run_key,
        tags={
            "trigger": "sensor",
            "sensor_name": "bronze_to_shared_staging_sensor",
            "triggered_by_assets": ",".join(materialized_assets),
            "pipeline_chain": "step_1_shared_staging",
        }
    )


# ==============================================================================
# STEP 2: SHARED STAGING DONE → DAILY TRANSFORM (both domains + shared dims)
# ==============================================================================

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dbt_shared_staging_job],
    request_jobs=[dbt_daily_transform_job],
    default_status=DEFAULT_SENSOR_STATUS,
    minimum_interval_seconds=30,
    description="Step 2/2: Shared staging done → launch full daily transform (both domains + dimensions)",
)
def shared_staging_to_domain_sensor(context: RunStatusSensorContext):
    """
    Fires AFTER dbt_shared_staging_job succeeds.

    Launches the single daily transform job, which builds the full Silver→Gold
    pipeline for both domains plus shared dimensions in one shot. dbt resolves
    intra-job ordering via the ref() DAG, so there is no fragile cross-job
    coordination (and no cursor — RunStatusSensorContext does not support one).
    """
    staging_run_id = context.dagster_run.run_id
    logger.info(
        f"Step 2/2: Shared staging completed (run {staging_run_id}). "
        f"Launching daily transform (both domains + dimensions)..."
    )

    yield RunRequest(
        run_key=f"daily_transform_{staging_run_id}",
        job_name=dbt_daily_transform_job.name,
        tags={
            "trigger": "sensor",
            "sensor_name": "shared_staging_to_domain_sensor",
            "parent_run_id": staging_run_id,
            "pipeline_chain": "step_2_daily_transform",
        }
    )


# ==============================================================================
# STEP 3: DAILY TRANSFORM DONE → CURRENT STANDARDIZED INDEX (IPS/SSFI)
# ==============================================================================

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dbt_daily_transform_job],
    request_jobs=[station_current_index_job],
    default_status=DEFAULT_SENSOR_STATUS,
    minimum_interval_seconds=30,
    description="Step 3/3: daily transform done → compute current standardized index (IPS/SSFI)",
)
def transform_to_index_sensor(context: RunStatusSensorContext):
    yield RunRequest(
        run_key=f"current_index_{context.dagster_run.run_id}",
        tags={
            "trigger": "sensor",
            "sensor_name": "transform_to_index_sensor",
            "pipeline_chain": "step_3_index",
        },
    )


# ==============================================================================
# EXPORTS
# ==============================================================================

all_sensors = [
    bronze_to_shared_staging_sensor,      # Step 1: Bronze → shared staging
    shared_staging_to_domain_sensor,      # Step 2: staging → daily transform
    transform_to_index_sensor,            # Step 3: daily transform → current index
]
