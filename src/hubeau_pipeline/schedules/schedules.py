"""
Schedules Dagster - Planification automatique des jobs
"""

from dagster import ScheduleDefinition

from ..jobs import (
    sync_all_stations,
    sync_all_yearly_data,
    piezometry_job,
    hydrometry_job,
    quality_rivers_job,
    quality_groundwater_job,
    ecoulement_job,
    hydrobio_job,
    prelevements_job,
    temperature_job,
)

# ====================================
# SCHEDULES RÉFÉRENTIELS (STATIONS)
# ====================================

# Mise à jour hebdomadaire des stations/sites de référence
# Les référentiels Hub'Eau changent peu, une mise à jour hebdomadaire suffit
sync_stations_weekly = ScheduleDefinition(
    job=sync_all_stations,
    cron_schedule="0 2 * * 0",  # Chaque dimanche à 2h du matin
    name="sync_stations_weekly",
    description="Mise à jour hebdomadaire de tous les référentiels de stations (8 APIs)",
)

# ====================================
# SCHEDULES DONNÉES TEMPORELLES
# ====================================

# Mise à jour mensuelle de l'année courante
# Pour capturer les nouvelles données du mois écoulé
sync_current_year_monthly = ScheduleDefinition(
    job=sync_all_yearly_data,
    cron_schedule="0 3 1 * *",  # 1er de chaque mois à 3h du matin
    name="sync_current_year_monthly",
    description="Mise à jour mensuelle des données de l'année courante (toutes APIs)",
)

# Backfill annuel complet (toutes les années depuis 2020)
# Pour assurer la complétude historique
sync_all_years_annually = ScheduleDefinition(
    job=sync_all_yearly_data,
    cron_schedule="0 4 15 1 *",  # 15 janvier à 4h (après le mensuel)
    name="sync_all_years_annually",
    description="Backfill annuel complet de toutes les années depuis 2020 (toutes APIs)",
)

# ====================================
# SCHEDULES PAR API (OPTIONNELS)
# ====================================
# Pour exécutions ciblées si besoin de fréquence différente

# Piézométrie - Données critiques, mise à jour bimensuelle
sync_piezometry_biweekly = ScheduleDefinition(
    job=piezometry_job,
    cron_schedule="0 3 1,15 * *",  # 1er et 15 de chaque mois à 3h
    name="sync_piezometry_biweekly",
    description="Mise à jour bimensuelle piézométrie (stations + chroniques année courante)",
)

# Qualité cours d'eau - Données analytiques, mise à jour mensuelle
sync_quality_rivers_monthly = ScheduleDefinition(
    job=quality_rivers_job,
    cron_schedule="0 3 5 * *",  # 5 de chaque mois à 3h
    name="sync_quality_rivers_monthly",
    description="Mise à jour mensuelle qualité cours d'eau (stations + analyses année courante)",
)

# Hydrométrie - Données volumineuses, mise à jour mensuelle
sync_hydrometry_monthly = ScheduleDefinition(
    job=hydrometry_job,
    cron_schedule="0 3 10 * *",  # 10 de chaque mois à 3h
    name="sync_hydrometry_monthly",
    description="Mise à jour mensuelle hydrométrie (stations + sites + observations année courante)",
)

# ====================================
# SCHEDULES ACTIFS
# ====================================

# Schedules de production activés par défaut
all_schedules = [
    # Référentiels (hebdomadaire)
    sync_stations_weekly,

    # Données temporelles (mensuel + annuel)
    sync_current_year_monthly,
    sync_all_years_annually,

    # APIs critiques (optionnel - décommenter si besoin)
    # sync_piezometry_biweekly,
    # sync_quality_rivers_monthly,
    # sync_hydrometry_monthly,
]

__all__ = [
    # Schedules référentiels
    "sync_stations_weekly",

    # Schedules données temporelles
    "sync_current_year_monthly",
    "sync_all_years_annually",

    # Schedules par API (optionnels)
    "sync_piezometry_biweekly",
    "sync_quality_rivers_monthly",
    "sync_hydrometry_monthly",

    # Tous les schedules
    "all_schedules"
]
