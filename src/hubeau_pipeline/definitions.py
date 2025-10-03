"""
Définitions Dagster centrales - Point d'entrée de l'application
"""

from dagster import Definitions

# Import des assets
from .assets import all_assets

# Import des jobs
from .jobs import all_jobs

# Import des schedules
from .schedules import all_schedules

# Import des capteurs
from .sensors import all_sensors

# Import des resources
from .resources import RESOURCES

# Définitions centrales
defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    resources=RESOURCES,
    sensors=all_sensors,
)
