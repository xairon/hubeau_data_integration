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
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from dagster.jobs import (
    sync_hubeau_daily_schedule,
    sync_hubeau_realtime_schedule,
    sync_hubeau_quality_schedule,
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

# Schedules nouvelle architecture dlt
dlt_schedules = [
    sync_hubeau_daily_schedule,      # Tous les jours à 4h (toutes les APIs)
    sync_hubeau_realtime_schedule,   # Toutes les heures (hydrométrie + piézométrie)
    sync_hubeau_quality_schedule,    # Tous les dimanches à 2h (qualité)
]

# ✅ NOUVELLE ARCHITECTURE: Utiliser les schedules dlt par défaut
all_schedules = dlt_schedules

__all__ = [
    # Schedules nouvelle architecture dlt (recommended)
    "dlt_schedules",
    "sync_hubeau_daily_schedule",
    "sync_hubeau_realtime_schedule",
    "sync_hubeau_quality_schedule",
    
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
