"""
Dagster Schedules - Automated Daily/Weekly/Monthly Data Integration

Schedules:
- daily_hubeau_bronze: 4h00 UTC - Hub'Eau API (piezo + hydro)
- daily_dbt_silver: 6h00 UTC - dbt Silver/Gold layer
- daily_era5_bronze: 3h00 UTC - ERA5 Smart Update
- monthly_reference_data: 1er du mois 2h00 UTC - BDLISA + Sandre nomenclatures
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
    dbt_silver_gold_pipeline_job,
    era5_meteo_job,
    era5_weekly_job,
    reference_data_bronze_job,
)


# ==============================================================================
# DAILY SCHEDULES - Hub'Eau Bronze
# ==============================================================================

daily_piezometry_schedule = ScheduleDefinition(
    job=daily_piezometry_bronze_job,
    cron_schedule="0 4 * * *",  # 4h00 UTC every day
    default_status=DefaultScheduleStatus.STOPPED,  # Manual activation in prod
    description="Daily: Piezometry chroniques (last 7 days)",
)

daily_hydrometry_schedule = ScheduleDefinition(
    job=daily_hydrometry_bronze_job,
    cron_schedule="0 4 * * *",  # 4h00 UTC every day
    default_status=DefaultScheduleStatus.STOPPED,
    description="Daily: Hydrometry observations (last 7 days)",
)


# ==============================================================================
# DAILY SCHEDULE - dbt Silver/Gold
# ==============================================================================

daily_dbt_schedule = ScheduleDefinition(
    job=dbt_silver_gold_pipeline_job,
    cron_schedule="0 6 * * *",  # 6h00 UTC (after Bronze completes)
    default_status=DefaultScheduleStatus.STOPPED,
    description="Daily: dbt Silver/Gold layer incremental refresh",
)


# ==============================================================================
# MONTHLY SCHEDULE - Reference Data (BDLISA + Sandre)
# ==============================================================================

monthly_reference_data_schedule = ScheduleDefinition(
    job=reference_data_bronze_job,
    cron_schedule="0 2 1 * *",  # 1er du mois à 2h00 UTC
    default_status=DefaultScheduleStatus.STOPPED,
    description="Monthly: BDLISA + Sandre nomenclatures (ref_*_eh)",
)

# ==============================================================================
# DAILY SCHEDULE - ERA5 Smart Update
# ==============================================================================

@schedule(
    job=era5_weekly_job,
    cron_schedule="0 3 * * *",  # Daily 3h00 UTC
    default_status=DefaultScheduleStatus.STOPPED,
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
# EXPORTS
# ==============================================================================

all_schedules = [
    daily_piezometry_schedule,
    daily_hydrometry_schedule,
    daily_dbt_schedule,
    daily_era5_schedule,
    monthly_reference_data_schedule,
]
