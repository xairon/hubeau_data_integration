"""
Définitions Dagster centrales - Point d'entrée de l'application
"""

from dagster import Definitions, define_asset_job, AssetSelection
from dagster._core.executor import multi_or_in_process_executor

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

# ============================================================================
# EXECUTOR CONFIG - Limite parallélisme pour économiser RAM
# ============================================================================
# Limite à 2 assets en parallèle (au lieu de 4 par défaut)
# Impact : Divise RAM peak par 2 (évite SIGKILL / OOM)
# ============================================================================

# Job par défaut avec executor limité à 2 workers
hubeau_job_limited = define_asset_job(
    name="hubeau_materialize_limited",
    selection=AssetSelection.all(),
    executor_def=multi_or_in_process_executor.configured({
        "max_concurrent": 2  # ✅ Max 2 assets en parallèle
    }),
    tags={"api": "hubeau"}
)

# Définitions centrales
defs = Definitions(
    assets=all_assets,
    jobs=all_jobs + [hubeau_job_limited],  # Ajouter le job avec executor limité
    schedules=all_schedules,
    resources=RESOURCES,
    sensors=all_sensors,
)
