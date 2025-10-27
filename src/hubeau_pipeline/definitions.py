"""
Définitions Dagster centrales - Point d'entrée de l'application
"""

from dagster import Definitions, define_asset_job, AssetSelection, multiprocess_executor

# Import des assets
from .assets import all_assets

# Import des jobs originaux ET améliorés
from .jobs import all_jobs
from .jobs.hubeau_jobs_improved import all_improved_jobs

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

# ✅ CHANGEMENT #4: Executor limité PAR DÉFAUT pour TOUS les runs
# Configuration globale de l'executor pour limiter le parallélisme
limited_executor = multiprocess_executor.configured({
    "max_concurrent": 2  # ✅ Max 2 assets en parallèle (au lieu de 4 par défaut)
})

# Job par défaut avec executor limité
hubeau_job_limited = define_asset_job(
    name="hubeau_materialize_limited",
    selection=AssetSelection.all(),
    executor_def=limited_executor,
    tags={"api": "hubeau"}
)

# Définitions centrales
defs = Definitions(
    assets=all_assets,
    jobs=all_jobs + all_improved_jobs + [hubeau_job_limited],  # Jobs originaux + améliorés + limité
    schedules=all_schedules,
    resources=RESOURCES,
    sensors=all_sensors,
    # ✅ CHANGEMENT #4: Forcer l'executor limité comme défaut global
    # Tous les jobs utilisent maintenant cet executor sauf override explicite
    executor=limited_executor,
)
