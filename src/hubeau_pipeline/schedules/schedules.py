"""
Schedules Dagster - Planification automatique des jobs
"""

from dagster import ScheduleDefinition

# Schedules ancienne architecture (deprecated - supprimés)
# from ..jobs import (
#     hubeau_hydrometry_job,
#     hubeau_piezometry_job,
#     hubeau_temperature_job,
#     hubeau_ecoulement_job,
#     hubeau_hydrobiology_job,
#     hubeau_prelevements_job,
# )

# Schedules nouvelle architecture dlt (recommended)
from ..jobs.dlt_jobs import (
    sync_all_stations,
    sync_all_yearly_data,
    sync_all_daily_data,
    sync_realtime_data,
)

# ====================================
# SCHEDULES ANCIENNE ARCHITECTURE (supprimés)
# ====================================

# old_schedules = []

# ====================================
# SCHEDULES NOUVELLE ARCHITECTURE DLT (recommended)
# ====================================

# Define schedules for dlt jobs
sync_all_yearly_data_schedule = ScheduleDefinition(
    job=sync_all_yearly_data,
    cron_schedule="0 3 1 1 *",  # Annually on January 1st at 3 AM
    name="sync_all_yearly_data_schedule",
    description="Annual schedule to synchronize yearly Hub'Eau data (quality, temperature, etc.).",
)

sync_all_daily_data_schedule = ScheduleDefinition(
    job=sync_all_daily_data,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    name="sync_all_daily_data_schedule",
    description="Daily schedule to synchronize daily Hub'Eau data (hydrometry + ecoulement).",
)

sync_realtime_data_schedule = ScheduleDefinition(
    job=sync_realtime_data,
    cron_schedule="0 * * * *",  # Hourly
    name="sync_realtime_data_schedule",
    description="Hourly schedule to synchronize real-time Hub'Eau data (hydrometry 30d).",
)

# Schedules nouvelle architecture dlt
dlt_schedules = [
    sync_all_yearly_data_schedule,   # 1er janvier à 3h (données annuelles)
    sync_all_daily_data_schedule,    # Tous les jours à 2h (données quotidiennes)
    sync_realtime_data_schedule,     # Toutes les heures (hydrométrie temps réel)
]

# ✅ NOUVELLE ARCHITECTURE: Utiliser les schedules dlt par défaut
all_schedules = dlt_schedules

__all__ = [
    # Schedules nouvelle architecture dlt (recommended)
    "dlt_schedules",
    "sync_all_yearly_data_schedule",
    "sync_all_daily_data_schedule",
    "sync_realtime_data_schedule",
    
    # Tous les schedules
    "all_schedules"
]
