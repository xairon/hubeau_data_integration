"""
Schedules Dagster - Planification automatique des jobs CSV
"""

from ..jobs.csv_jobs import (
    daily_chroniques_schedule,
    weekly_stations_schedule,
)

# ====================================
# SCHEDULES CSV
# ====================================

# Schedules définis dans csv_jobs.py:
# - daily_chroniques_schedule: Tous les jours à 02h00 (incremental, 2 jours)
# - weekly_stations_schedule: Dimanches à 03h00 (full refresh)

# ====================================
# SCHEDULES ACTIFS
# ====================================

# Schedules CSV de production
all_schedules = [
    daily_chroniques_schedule,      # Quotidien: chroniques/analyses (incremental)
    weekly_stations_schedule,       # Hebdomadaire: stations/référentiels (full)
]

__all__ = [
    # Schedules CSV
    "daily_chroniques_schedule",
    "weekly_stations_schedule",

    # Tous les schedules
    "all_schedules"
]
