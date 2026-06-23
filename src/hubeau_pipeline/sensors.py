"""
Dagster Sensors - Event-Driven Pipeline Orchestration

Chain:
  Bronze materializes
    → bronze_to_transform_sensor    → dbt_transform_job (ALL models, incremental)
    → transform_to_index_sensor     → station_index_refresh (IPS/SSFI)
    → transform_to_quality_sensor    → dbt_quality_job (freshness + tests)

The two post-transform sensors fire in parallel on dbt_transform_job SUCCESS:
the index refresh (data) and the quality checks (non-blocking — a failing test
surfaces as a failed run, it does NOT block the data refresh).

History: an earlier design split the transform into shared_staging → daily_transform
with a cursor-tracking RunStatusSensor rejoin. RunStatusSensorContext has no cursor,
so the rejoin crashed every tick and silently stopped refreshing aggregates/dimensions.
Runs are serialized globally (max_concurrent_runs=1), so the split gave no speedup —
it is now a single dbt_transform_job and the chain is robust.
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
    dbt_quality_job,
    dbt_transform_job,
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
# STEP 1: BRONZE → DBT TRANSFORM (all models)
# ==============================================================================

@multi_asset_sensor(
    monitored_assets=[
        AssetKey("piezometry_chroniques_daily_raw"),
        AssetKey("hydrometry_obs_daily_raw"),
    ],
    job=dbt_transform_job,
    minimum_interval_seconds=300,  # 5 min cooldown
    default_status=DEFAULT_SENSOR_STATUS,
    description="Step 1/2: Bronze materializes → launch full dbt transform (all models)",
)
def bronze_to_transform_sensor(context: MultiAssetSensorEvaluationContext):
    """Entry point: watches Bronze daily chroniques, launches the full dbt transform."""
    asset_events = context.latest_materialization_records_by_key()

    materialized_assets = []
    for asset_key, record in asset_events.items():
        if record is not None:
            materialized_assets.append(asset_key.to_user_string())

    # Always advance all cursors to prevent trailing_unconsumed_events overflow.
    # Must be called whether we yield a RunRequest or skip.
    context.advance_all_cursors()

    if not materialized_assets:
        return SkipReason("No new Bronze materializations detected")

    # Date-based run_key: piezo and hydro materialize at different times but should
    # only trigger ONE transform per day.
    logger.info(
        f"Step 1/2: Bronze materialized ({', '.join(materialized_assets)}). "
        f"Launching dbt transform..."
    )
    yield RunRequest(
        run_key=f"dbt_transform_{date.today().isoformat()}",
        tags={
            "trigger": "sensor",
            "sensor_name": "bronze_to_transform_sensor",
            "triggered_by_assets": ",".join(materialized_assets),
            "pipeline_chain": "step_1_transform",
        },
    )


# ==============================================================================
# STEP 2a: TRANSFORM DONE → CURRENT STANDARDIZED INDEX (IPS/SSFI)
# ==============================================================================

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dbt_transform_job],
    request_jobs=[station_current_index_job],
    default_status=DEFAULT_SENSOR_STATUS,
    minimum_interval_seconds=30,
    description="Step 2/2: transform done → compute current standardized index (IPS/SSFI)",
)
def transform_to_index_sensor(context: RunStatusSensorContext):
    yield RunRequest(
        run_key=f"current_index_{context.dagster_run.run_id}",
        tags={
            "trigger": "sensor",
            "sensor_name": "transform_to_index_sensor",
            "pipeline_chain": "step_2_index",
        },
    )


# ==============================================================================
# STEP 2b: TRANSFORM DONE → DATA QUALITY (freshness + dbt tests), non-blocking
# ==============================================================================

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dbt_transform_job],
    request_jobs=[dbt_quality_job],
    default_status=DEFAULT_SENSOR_STATUS,
    minimum_interval_seconds=30,
    description="Step 2/2: transform done → run source freshness + dbt tests (non-blocking alerting)",
)
def transform_to_quality_sensor(context: RunStatusSensorContext):
    yield RunRequest(
        run_key=f"quality_{context.dagster_run.run_id}",
        tags={
            "trigger": "sensor",
            "sensor_name": "transform_to_quality_sensor",
            "pipeline_chain": "step_2_quality",
        },
    )


# ==============================================================================
# EXPORTS
# ==============================================================================

all_sensors = [
    bronze_to_transform_sensor,    # Step 1: Bronze → dbt transform (all models)
    transform_to_index_sensor,     # Step 2a: transform → current index
    transform_to_quality_sensor,   # Step 2b: transform → quality (tests + freshness)
]
