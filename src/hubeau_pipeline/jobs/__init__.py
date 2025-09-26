"""
Jobs Bronze Simplifiés - 3 jobs essentiels seulement
Organisation claire par source de données
"""

# Import des 3 jobs essentiels
from .bronze_simple_jobs import (
    hubeau_bronze_job,
    bdlisa_bronze_job, 
    sandre_bronze_job
)

# Jobs Bronze - 3 seulement !
bronze_jobs = [
    hubeau_bronze_job,   # 8 APIs Hub'Eau
    bdlisa_bronze_job,   # Géologie BDLISA
    sandre_bronze_job    # Nomenclatures Sandre
]

# Tous les jobs
all_jobs = bronze_jobs

__all__ = [
    "all_jobs",
    "bronze_jobs",
    "hubeau_bronze_job",
    "bdlisa_bronze_job", 
    "sandre_bronze_job"
]