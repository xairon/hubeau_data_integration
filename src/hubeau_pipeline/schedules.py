"""
Dagster Schedules - Automated Daily/Weekly/Monthly Data Integration

Schedule Timeline (UTC) - generous time gaps between stages:
- 3h00: ERA5 Smart Update (Bronze)
- 4h00: Hub'Eau Bronze (piezo + hydro in parallel)
- 5h00: dbt Shared Staging (ERA5 timeseries + grid points) [1h buffer]
- 6h30: dbt Domain Pipelines (piezo + hydro in PARALLEL) [1h30 buffer]
- 8h00: dbt Shared Dimensions (dim_date, dim_geography) [1h30 buffer]

Monthly:
- 1er du mois 2h00: BDLISA + Sandre reference data

Weekly:
- Dimanche 5h00: dbt documentation generation
"""

from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus,
    schedule,
    RunRequest,
    ScheduleEvaluationContext,
)
from datetime import datetime

from .jobs import (
    daily_piezometry_bronze_job,
    daily_hydrometry_bronze_job,
    dbt_shared_staging_job,
    dbt_piezo_pipeline_job,
    dbt_piezo_pipeline_daily_job,
    dbt_hydro_pipeline_job,
    dbt_hydro_pipeline_daily_job,
    dbt_shared_dimensions_job,
    era5_weekly_job,
    reference_data_bronze_job,
    dbt_docs_job,
)
from .utils import env_true


DEFAULT_SCHEDULE_STATUS = (
    DefaultScheduleStatus.RUNNING
    if env_true("DAGSTER_ENABLE_SCHEDULES", "false")
    else DefaultScheduleStatus.STOPPED
)


# ==============================================================================
# DAILY SCHEDULES - Hub'Eau Bronze
# ==============================================================================

daily_piezometry_schedule = ScheduleDefinition(
    job=daily_piezometry_bronze_job,
    cron_schedule="0 4 * * *",  # 4h00 UTC every day
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Daily: Piezometry chroniques (last 7 days)",
)

daily_hydrometry_schedule = ScheduleDefinition(
    job=daily_hydrometry_bronze_job,
    cron_schedule="0 4 * * *",  # 4h00 UTC every day
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Daily: Hydrometry observations (last 7 days)",
)


# ==============================================================================
# DAILY SCHEDULES - dbt Pipelines (4-stage execution)
# ==============================================================================

# Stage 1: Shared staging (ERA5 data) - runs at 5h00 UTC
# Must complete before domain pipelines can start
# WARNING: Time-based scheduling assumes previous stage completed within the gap.
# If Bronze (4h00) takes >1h, this stage may process stale data.
# Consider switching to sensor-based triggering for production reliability.
daily_dbt_shared_staging_schedule = ScheduleDefinition(
    job=dbt_shared_staging_job,
    cron_schedule="0 5 * * *",  # 5h00 UTC (1h buffer after Bronze)
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Daily: dbt shared staging (ERA5 timeseries + grid points)",
)

# Stage 2a: Piezo pipeline runs at 6h30 UTC (after shared staging)
daily_dbt_piezo_schedule = ScheduleDefinition(
    job=dbt_piezo_pipeline_daily_job,
    cron_schedule="30 6 * * *",  # 6h30 UTC (1h30 buffer after stage 1)
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Daily: dbt Piezometry pipeline (streaming-optimized)",
)

# Stage 2b: Hydro pipeline runs at 6h30 UTC (after shared staging)
# Runs IN PARALLEL with piezo pipeline (different concurrency keys)
daily_dbt_hydro_schedule = ScheduleDefinition(
    job=dbt_hydro_pipeline_daily_job,
    cron_schedule="30 6 * * *",  # 6h30 UTC (1h30 buffer after stage 1)
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Daily: dbt Hydrometry pipeline (streaming-optimized)",
)

# Stage 3: Shared dimensions run at 8h00 UTC (after both domain pipelines complete)
daily_dbt_dimensions_schedule = ScheduleDefinition(
    job=dbt_shared_dimensions_job,
    cron_schedule="0 8 * * *",  # 8h00 UTC (1h30 buffer after domain pipelines)
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Daily: dbt shared dimensions (dim_date, dim_geography)",
)


# ==============================================================================
# MONTHLY SCHEDULE - Reference Data (BDLISA + Sandre)
# ==============================================================================

monthly_reference_data_schedule = ScheduleDefinition(
    job=reference_data_bronze_job,
    cron_schedule="0 2 1 * *",  # 1er du mois à 2h00 UTC
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Monthly: BDLISA + Sandre nomenclatures (ref_*_eh)",
)

# ==============================================================================
# DAILY SCHEDULE - ERA5 Smart Update
# ==============================================================================

@schedule(
    job=era5_weekly_job,
    cron_schedule="0 3 * * *",  # Daily 3h00 UTC
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Daily: ERA5 Smart Update (Target Timeseries)",
)
def daily_era5_schedule(context: ScheduleEvaluationContext):
    """
    ERA5 Daily schedule.
    Lance le job de mise à jour incrémentale.
    L'asset 'era5_weekly_update' calcule lui-même la période manquante (Smart Update).
    """
    return RunRequest(
        run_key=f"era5_daily_{datetime.now().strftime('%Y%m%d')}",
    )


# ==============================================================================
# WEEKLY SCHEDULE - dbt Documentation Generation
# ==============================================================================

weekly_dbt_docs_schedule = ScheduleDefinition(
    job=dbt_docs_job,
    cron_schedule="0 5 * * 0",  # Dimanche 5h00 UTC (hebdomadaire)
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Weekly: Generate dbt documentation (catalog.json + manifest.json)",
)


# ==============================================================================
# EXPORTS
# ==============================================================================

all_schedules = [
    # Bronze layer (data ingestion)
    daily_piezometry_schedule,
    daily_hydrometry_schedule,
    daily_era5_schedule,
    # dbt Stage 1: shared staging (ERA5)
    daily_dbt_shared_staging_schedule,
    # dbt Stage 2: domain pipelines (parallel)
    daily_dbt_piezo_schedule,
    daily_dbt_hydro_schedule,
    # dbt Stage 3: shared dimensions
    daily_dbt_dimensions_schedule,
    # Documentation & reference data
    weekly_dbt_docs_schedule,
    monthly_reference_data_schedule,
]
