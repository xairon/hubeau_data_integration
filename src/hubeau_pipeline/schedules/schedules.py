"""
Définition des schedules pour l'orchestration
Schedules adaptés aux fréquences réelles des données
"""

from dagster import ScheduleDefinition

# Import des jobs
from hubeau_pipeline.jobs import (
    bdlisa_bronze_job,
    hubeau_hydrobiology_job,
    hubeau_hydrometry_job,
    hubeau_onde_job,
    hubeau_piezometry_job,
    hubeau_prelevements_job,
    hubeau_temperature_job,
    hubeau_water_quality_groundwater_job,
    hubeau_water_quality_surface_job,
    sandre_bronze_job,
)

# ================================
# SCHEDULES QUOTIDIENS (Séries continues)
# ================================

hydrometry_schedule = ScheduleDefinition(
    job=hubeau_hydrometry_job,
    cron_schedule="0 6 * * *",  # Quotidien 6h
    execution_timezone="Europe/Paris",
    name="hydrometry_schedule",
    description="🌊 Hydrométrie: 30 derniers jours automatique (quotidien)"
)

piezometry_schedule = ScheduleDefinition(
    job=hubeau_piezometry_job,
    cron_schedule="0 6 * * *",  # Quotidien 6h
    execution_timezone="Europe/Paris",
    name="piezometry_schedule",
    description="🏔️ Piézométrie: Niveaux nappes (quotidien)"
)

temperature_schedule = ScheduleDefinition(
    job=hubeau_temperature_job,
    cron_schedule="0 6 * * *",  # Quotidien 6h
    execution_timezone="Europe/Paris",
    name="temperature_schedule",
    description="🌡️ Température: Mesures horaires (quotidien)"
)

# ================================
# SCHEDULES ANNUELS (Campagnes)
# ================================

onde_schedule = ScheduleDefinition(
    job=hubeau_onde_job,
    cron_schedule="0 7 15 1 *",  # 15 janvier 7h (campagnes estivales année précédente)
    execution_timezone="Europe/Paris",
    name="onde_schedule",
    description="🌊 ONDE: Campagnes estivales (annuel)"
)

water_quality_surface_schedule = ScheduleDefinition(
    job=hubeau_water_quality_surface_job,
    cron_schedule="0 8 15 1 *",  # 15 janvier 8h (prélèvements année précédente)
    execution_timezone="Europe/Paris",
    name="water_quality_surface_schedule",
    description="🧪 Qualité Cours d'Eau: Récupération annuelle (annuel)"
)

water_quality_groundwater_schedule = ScheduleDefinition(
    job=hubeau_water_quality_groundwater_job,
    cron_schedule="0 8 15 1 *",  # 15 janvier 8h (prélèvements année précédente)
    execution_timezone="Europe/Paris",
    name="water_quality_groundwater_schedule",
    description="🧪 Qualité Nappes: Récupération annuelle (annuel)"
)

hydrobiology_schedule = ScheduleDefinition(
    job=hubeau_hydrobiology_job,
    cron_schedule="0 10 15 1 *",  # 15 janvier 10h (campagnes année précédente)
    execution_timezone="Europe/Paris",
    name="hydrobiology_schedule",
    description="🐟 Hydrobiologie: Campagnes saisonnières (annuel)"
)

prelevements_schedule = ScheduleDefinition(
    job=hubeau_prelevements_job,
    cron_schedule="0 9 15 1 *",  # 15 janvier 9h (déclarations annuelles)
    execution_timezone="Europe/Paris",
    name="prelevements_schedule",
    description="💧 Prélèvements: Déclarations annuelles (annuel)"
)

# ================================
# SCHEDULES EXTERNES
# ================================

bdlisa_schedule = ScheduleDefinition(
    job=bdlisa_bronze_job,
    cron_schedule="0 8 1 * *",  # Premier du mois 8h
    execution_timezone="Europe/Paris",
    name="bdlisa_schedule",
    description="🗺️ BDLISA: Géologie (mensuel)"
)

sandre_schedule = ScheduleDefinition(
    job=sandre_bronze_job,
    cron_schedule="0 9 1 * *",  # Premier du mois 9h
    execution_timezone="Europe/Paris",
    name="sandre_schedule",
    description="📚 Sandre: Nomenclatures (mensuel)"
)

# ================================
# EXPORTS
# ================================

all_schedules = [
    # Quotidiens (séries continues)
    hydrometry_schedule,
    piezometry_schedule,
    temperature_schedule,
    # Annuels (campagnes + déclarations)
    onde_schedule,
    water_quality_surface_schedule,
    water_quality_groundwater_schedule,
    hydrobiology_schedule,
    prelevements_schedule,
    # Externes
    bdlisa_schedule,
    sandre_schedule,
]
