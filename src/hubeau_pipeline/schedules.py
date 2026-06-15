"""
Dagster Schedules - Data Ingestion + Maintenance

Schedules handle INGESTION only (Bronze layer). The dbt transformation chain
is triggered by SENSORS (event-driven) after Bronze materializes:
  Bronze done → sensor → shared staging → sensor → domain pipelines → sensor → dimensions

Schedule Timeline (UTC):
- 3h00: ERA5 Smart Update (Bronze)
- 4h00: Hub'Eau Bronze (piezo + hydro in parallel) → triggers sensor chain
- 1er du mois 2h00: Reference data (BDLISA TME)
- Dimanche 5h00: dbt documentation generation
- Dimanche 7h00: IPS reference baseline (station_reference_stats)
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
    data_completeness_job,
    era5_weekly_job,
    reference_data_bronze_job,
    station_reference_stats_job,
    dbt_docs_job,
)
from .utils import env_true


DEFAULT_SCHEDULE_STATUS = (
    DefaultScheduleStatus.RUNNING
    if env_true("DAGSTER_ENABLE_SCHEDULES", "false")
    else DefaultScheduleStatus.STOPPED
)


# ==============================================================================
# DAILY SCHEDULES - Hub'Eau Bronze Ingestion
# ==============================================================================
# After these complete, sensors automatically trigger the dbt chain:
# shared_staging → piezo + hydro (parallel) → dimensions

daily_piezometry_schedule = ScheduleDefinition(
    job=daily_piezometry_bronze_job,
    cron_schedule="0 4 * * *",  # 4h00 UTC every day
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Daily: Piezometry chroniques (last 7 days) → triggers dbt sensor chain",
)

daily_hydrometry_schedule = ScheduleDefinition(
    job=daily_hydrometry_bronze_job,
    cron_schedule="0 4 * * *",  # 4h00 UTC every day
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Daily: Hydrometry observations (last 7 days) → triggers dbt sensor chain",
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
    """ERA5 incremental update. Calculates missing period automatically."""
    return RunRequest(
        run_key=f"era5_daily_{datetime.now().strftime('%Y%m%d')}",
    )


# ==============================================================================
# MONTHLY SCHEDULE - Reference Data (BDLISA + Sandre)
# ==============================================================================

monthly_reference_data_schedule = ScheduleDefinition(
    job=reference_data_bronze_job,
    cron_schedule="0 2 1 * *",  # 1er du mois à 2h00 UTC
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Monthly: BDLISA TME hydrogeo entities",
)


# ==============================================================================
# WEEKLY SCHEDULE - dbt Documentation Generation
# ==============================================================================

weekly_dbt_docs_schedule = ScheduleDefinition(
    job=dbt_docs_job,
    cron_schedule="0 5 * * 0",  # Dimanche 5h00 UTC
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Weekly: Generate dbt documentation (catalog.json + manifest.json)",
)


# ==============================================================================
# WEEKLY SCHEDULE - Data Completeness Check
# ==============================================================================

weekly_completeness_schedule = ScheduleDefinition(
    job=data_completeness_job,
    cron_schedule="0 6 * * 1",  # Lundi 6h00 UTC, après les ingestions de 4h
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Weekly: detect missing/partial months in silver chroniques and gold monthly index",
)


# ==============================================================================
# WEEKLY SCHEDULE - IPS reference baseline (station_reference_stats)
# ==============================================================================
# Baseline pluriannuelle (grilles de quantiles) lente à varier : recalcul hebdo
# suffit. fct_monthly_index + station_current_index (qui la lisent) sont, eux,
# reconstruits chaque nuit par la chaîne sensor (station_current_index_job).

weekly_reference_stats_schedule = ScheduleDefinition(
    job=station_reference_stats_job,
    cron_schedule="0 7 * * 0",  # Dimanche 7h00 UTC
    default_status=DEFAULT_SCHEDULE_STATUS,
    description="Weekly: recompute IPS reference baseline gold.station_reference_stats",
)


# ==============================================================================
# EXPORTS
# ==============================================================================

all_schedules = [
    # Bronze ingestion (triggers sensor chain for dbt)
    daily_piezometry_schedule,
    daily_hydrometry_schedule,
    daily_era5_schedule,
    # Maintenance
    weekly_dbt_docs_schedule,
    monthly_reference_data_schedule,
    # Qualité
    weekly_completeness_schedule,
    # Indices - baseline IPS hebdomadaire
    weekly_reference_stats_schedule,
]
