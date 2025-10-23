"""
Schedules Dagster - Planification automatique des jobs CSV
"""

from ..jobs.csv_incremental_jobs import (
    daily_chroniques_schedule,
    weekly_references_schedule,
)

# ====================================
# SCHEDULES CSV (NOUVELLE ARCHITECTURE)
# ====================================

# Les schedules sont définis dans csv_incremental_jobs.py:
# - daily_chroniques_schedule: Tous les jours à 02h00 (derniers 2 jours)
# - weekly_references_schedule: Tous les dimanches à 03h00 (full refresh)

# ====================================
# SCHEDULES ACTIFS
# ====================================

# Schedules CSV de production
all_schedules = [
    daily_chroniques_schedule,      # Quotidien: chroniques/analyses
    weekly_references_schedule,     # Hebdomadaire: stations/référentiels
]

__all__ = [
    # Schedules CSV
    "daily_chroniques_schedule",
    "weekly_references_schedule",

    # Tous les schedules
    "all_schedules"
]
