"""
Définition des schedules pour l'orchestration
"""

from dagster import ScheduleDefinition
# Anciens jobs temporairement désactivés
# from hubeau_pipeline.jobs import (
#     hubeau_production_job, 
#     bdlisa_production_job, 
#     sandre_production_job,
#     analytics_production_job
# )

# Jobs Bronze simplifiés
from hubeau_pipeline.jobs import hubeau_bronze_job, bdlisa_bronze_job, sandre_bronze_job

# Schedule Hub'Eau quotidien
hubeau_schedule = ScheduleDefinition(
    job=hubeau_bronze_job,
    cron_schedule="0 6 * * *",  # Quotidien 6h
    execution_timezone="Europe/Paris",
    name="hubeau_schedule",
    description="🌊 Hub'Eau: 8 APIs quotidiennes"
)

# Schedule BDLISA mensuel  
bdlisa_schedule = ScheduleDefinition(
    job=bdlisa_bronze_job,
    cron_schedule="0 8 1 * *",  # Premier du mois 8h
    execution_timezone="Europe/Paris", 
    name="bdlisa_schedule",
    description="🗺️ BDLISA: Géologie mensuelle"
)

# Schedule Sandre mensuel
sandre_schedule = ScheduleDefinition(
    job=sandre_bronze_job,
    cron_schedule="0 9 1 * *",  # Premier du mois 9h
    execution_timezone="Europe/Paris",
    name="sandre_schedule", 
    description="📚 Sandre: Nomenclatures mensuelles"
)

# Anciens schedules (à réactiver quand jobs prêts)
# hubeau_schedule = ScheduleDefinition(...)
# bdlisa_schedule = ScheduleDefinition(...)
# sandre_schedule = ScheduleDefinition(...)
# analytics_schedule = ScheduleDefinition(...)

# Liste de tous les schedules - 3 schedules simplifiés
all_schedules = [
    hubeau_schedule,
    bdlisa_schedule,
    sandre_schedule
]
