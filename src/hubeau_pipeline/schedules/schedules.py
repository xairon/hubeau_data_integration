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
from ..jobs import (
    sync_all_stations,
    sync_all_yearly_data,
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

# ✅ SIMPLIFIÉ: Toutes les données utilisent maintenant des partitions annuelles
# Schedules nouvelle architecture dlt
dlt_schedules = [
    sync_all_yearly_data_schedule,   # 1er janvier à 3h (données annuelles)
]

# ✅ NOUVELLE ARCHITECTURE: Utiliser les schedules dlt par défaut
all_schedules = dlt_schedules

__all__ = [
    # Schedules nouvelle architecture dlt (recommended)
    "dlt_schedules",
    "sync_all_yearly_data_schedule",
    
    # Tous les schedules
    "all_schedules"
]
