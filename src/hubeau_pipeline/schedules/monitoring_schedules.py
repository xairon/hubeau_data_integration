"""
Schedules de monitoring Hub'Eau
"""

from dagster import ScheduleDefinition

from ..jobs.monitoring_jobs import (
    data_quality_monitoring_job,
    performance_monitoring_job,
    full_monitoring_job,
    quick_monitoring_job
)

# ====================================
# SCHEDULES DE MONITORING
# ====================================

# Monitoring rapide toutes les heures
quick_monitoring_hourly = ScheduleDefinition(
    job=quick_monitoring_job,
    cron_schedule="0 * * * *",  # Toutes les heures
    name="quick_monitoring_hourly",
    description="Monitoring rapide des métriques essentielles toutes les heures",
)

# Monitoring qualité des données quotidien
data_quality_daily = ScheduleDefinition(
    job=data_quality_monitoring_job,
    cron_schedule="0 6 * * *",  # Tous les jours à 6h du matin
    name="data_quality_daily",
    description="Monitoring complet de la qualité des données quotidien",
)

# Monitoring performance quotidien
performance_monitoring_daily = ScheduleDefinition(
    job=performance_monitoring_job,
    cron_schedule="0 7 * * *",  # Tous les jours à 7h du matin
    name="performance_monitoring_daily",
    description="Monitoring des performances quotidien",
)

# Dashboard complet hebdomadaire
full_monitoring_weekly = ScheduleDefinition(
    job=full_monitoring_job,
    cron_schedule="0 8 * * 0",  # Tous les dimanches à 8h du matin
    name="full_monitoring_weekly",
    description="Dashboard complet de monitoring hebdomadaire",
)

# Monitoring intensif en semaine (pour production)
intensive_monitoring_weekdays = ScheduleDefinition(
    job=full_monitoring_job,
    cron_schedule="0 */4 * * 1-5",  # Toutes les 4h en semaine
    name="intensive_monitoring_weekdays",
    description="Monitoring intensif toutes les 4h en semaine pour production",
)
