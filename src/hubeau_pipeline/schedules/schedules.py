"""
Schedules Dagster - Planification automatique des jobs
"""

from dagster import ScheduleDefinition

# Schedules ancienne architecture (deprecated)
from ..jobs import (
    hubeau_hydrometry_job,
    hubeau_piezometry_job,
    hubeau_temperature_job,
    hubeau_ecoulement_job,
    hubeau_hydrobiology_job,
    hubeau_prelevements_job,
)

# Schedules nouvelle architecture dlt (recommended)
from ..jobs.dlt_jobs import (
    sync_all_stations,
    sync_all_yearly_data,
    sync_realtime_data,
)

# ====================================
# SCHEDULES ANCIENNE ARCHITECTURE (deprecated)
# ====================================

hydrometry_schedule = ScheduleDefinition(
    job=hubeau_hydrometry_job,
    cron_schedule="0 */2 * * *",  # Toutes les 2 heures
)

piezometry_schedule = ScheduleDefinition(
    job=hubeau_piezometry_job,
    cron_schedule="30 */4 * * *",  # Toutes les 4 heures
)

temperature_schedule = ScheduleDefinition(
    job=hubeau_temperature_job,
    cron_schedule="0 6 * * *",  # Tous les jours à 6h
)

ecoulement_schedule = ScheduleDefinition(
    job=hubeau_ecoulement_job,
    cron_schedule="0 5 * * *",  # Tous les jours à 5h
)

hydrobiology_schedule = ScheduleDefinition(
    job=hubeau_hydrobiology_job,
    cron_schedule="0 3 * * *",  # Tous les jours à 3h
)

prelevements_schedule = ScheduleDefinition(
    job=hubeau_prelevements_job,
    cron_schedule="0 7 * * *",  # Tous les jours à 7h
)

# Schedules ancienne architecture
old_schedules = [
    hydrometry_schedule,
    piezometry_schedule,
    temperature_schedule,
    ecoulement_schedule,
    hydrobiology_schedule,
    prelevements_schedule,
]

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

sync_realtime_data_schedule = ScheduleDefinition(
    job=sync_realtime_data,
    cron_schedule="0 * * * *",  # Hourly
    name="sync_realtime_data_schedule",
    description="Hourly schedule to synchronize real-time Hub'Eau data (hydrometry 30d).",
)

# Schedules nouvelle architecture dlt
dlt_schedules = [
    sync_all_yearly_data_schedule,   # 1er janvier à 3h (données annuelles)
    sync_realtime_data_schedule,     # Toutes les heures (hydrométrie temps réel)
]

# ✅ NOUVELLE ARCHITECTURE: Utiliser les schedules dlt par défaut
all_schedules = dlt_schedules

__all__ = [
    # Schedules nouvelle architecture dlt (recommended)
    "dlt_schedules",
    "sync_all_yearly_data_schedule",
    "sync_realtime_data_schedule",
    
    # Schedules ancienne architecture (deprecated)
    "old_schedules",
    "hydrometry_schedule",
    "piezometry_schedule",
    "temperature_schedule",
    "ecoulement_schedule",
    "hydrobiology_schedule",
    "prelevements_schedule",
    
    # Tous les schedules
    "all_schedules"
]
