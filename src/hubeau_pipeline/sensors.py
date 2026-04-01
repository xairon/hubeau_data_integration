"""
Dagster Sensors - Event-Driven Pipeline Orchestration

Architecture:
  Bronze materializes → sensor detects → launches sequential dbt chain:
    1. Shared staging (ERA5) — MUST complete first
    2. Domain pipelines (piezo + hydro in parallel) — depend on shared staging
    3. Shared dimensions — depend on both domain pipelines

Single sensor replaces the fragile 4-stage time-based schedule.
Each step only starts after its prerequisite completes successfully.
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
    dbt_hydro_pipeline_daily_job,
    dbt_piezo_pipeline_daily_job,
    dbt_shared_dimensions_job,
    dbt_shared_staging_job,
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
# STEP 2: SHARED STAGING DONE → DOMAIN PIPELINES (PARALLEL)
# ==============================================================================

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dbt_shared_staging_job],
    request_jobs=[dbt_piezo_pipeline_daily_job, dbt_hydro_pipeline_daily_job],
    default_status=DEFAULT_SENSOR_STATUS,
    minimum_interval_seconds=30,
    description="Step 2/3: Shared staging done → launch piezo + hydro pipelines in parallel",
)
def shared_staging_to_domain_sensor(context: RunStatusSensorContext):
    """
    Fires AFTER dbt_shared_staging_job succeeds.
    Launches both domain pipelines in parallel (different concurrency keys).
    """
    staging_run_id = context.dagster_run.run_id
    logger.info(
        f"Step 2/3: Shared staging completed (run {staging_run_id}). "
        f"Launching piezo + hydro domain pipelines..."
    )

    # Launch both domain pipelines — they have different concurrency keys
    # so Dagster will run them in parallel
    yield RunRequest(
        run_key=f"piezo_daily_{staging_run_id}",
        job_name=dbt_piezo_pipeline_daily_job.name,
        tags={
            "trigger": "sensor",
            "sensor_name": "shared_staging_to_domain_sensor",
            "parent_run_id": staging_run_id,
            "pipeline_chain": "step_2_piezo",
        }
    )
    yield RunRequest(
        run_key=f"hydro_daily_{staging_run_id}",
        job_name=dbt_hydro_pipeline_daily_job.name,
        tags={
            "trigger": "sensor",
            "sensor_name": "shared_staging_to_domain_sensor",
            "parent_run_id": staging_run_id,
            "pipeline_chain": "step_2_hydro",
        }
    )


# ==============================================================================
# STEP 3: DOMAIN PIPELINES DONE → SHARED DIMENSIONS
# ==============================================================================

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dbt_piezo_pipeline_daily_job, dbt_hydro_pipeline_daily_job],
    request_jobs=[dbt_shared_dimensions_job],
    default_status=DEFAULT_SENSOR_STATUS,
    minimum_interval_seconds=30,
    description="Step 3/3: Both domain pipelines done → launch shared dimensions",
)
def domain_to_dimensions_sensor(context: RunStatusSensorContext):
    """
    Fires when a domain pipeline succeeds. Only launches dimensions
    when BOTH piezo and hydro have completed for the same chain.

    Uses cursor to track which domain pipelines have completed.
    """
    completed_run = context.dagster_run
    completed_job = completed_run.job_name
    parent_run_id = completed_run.tags.get("parent_run_id", "unknown")

    # Track completion state in cursor: "parent_run_id:piezo|hydro|both"
    cursor = context.cursor or ""

    # Parse cursor: format is "parent_run_id:completed_domains"
    cursor_parent, _, cursor_domains = cursor.partition(":")

    if cursor_parent != parent_run_id:
        # New chain — reset tracking
        cursor_domains = ""

    # Record which domain just completed
    if completed_job == dbt_piezo_pipeline_daily_job.name:
        if "piezo" not in cursor_domains:
            cursor_domains = f"{cursor_domains},piezo" if cursor_domains else "piezo"
    elif completed_job == dbt_hydro_pipeline_daily_job.name:
        if "hydro" not in cursor_domains:
            cursor_domains = f"{cursor_domains},hydro" if cursor_domains else "hydro"

    context.update_cursor(f"{parent_run_id}:{cursor_domains}")

    # Only launch dimensions when BOTH are done
    if "piezo" in cursor_domains and "hydro" in cursor_domains:
        logger.info(
            f"Step 3/3: Both domain pipelines completed (chain {parent_run_id}). "
            f"Launching shared dimensions..."
        )
        yield RunRequest(
            run_key=f"dimensions_{parent_run_id}",
            tags={
                "trigger": "sensor",
                "sensor_name": "domain_to_dimensions_sensor",
                "parent_run_id": parent_run_id,
                "pipeline_chain": "step_3_dimensions",
            }
        )
    else:
        missing = "hydro" if "piezo" in cursor_domains else "piezo"
        logger.info(
            f"Step 3/3: {completed_job} done, waiting for {missing} pipeline..."
        )
        return SkipReason(
            f"{completed_job} completed, waiting for {missing} to finish"
        )


# ==============================================================================
# EXPORTS
# ==============================================================================

all_sensors = [
    bronze_to_shared_staging_sensor,      # Step 1: Bronze → shared staging
    shared_staging_to_domain_sensor,      # Step 2: staging → piezo + hydro
    domain_to_dimensions_sensor,          # Step 3: piezo + hydro → dimensions
]
