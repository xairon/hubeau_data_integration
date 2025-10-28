"""
Définitions Dagster centrales - Point d'entrée de l'application
"""

from dagster import Definitions, define_asset_job, AssetSelection, multiprocess_executor

# Import des assets
from .assets import all_assets

# Import des jobs et schedules
from .jobs import all_jobs, all_schedules as hubeau_schedules

# Import des schedules (vide actuellement - pour extensions futures)
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

# Définitions centrales
defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,  # 12 jobs : 8 par API + 2 globaux + 2 tests
    schedules=all_schedules + hubeau_schedules,  # Schedules génériques + schedules Hub'Eau
    resources=RESOURCES,
    sensors=all_sensors,
    # ✅ Executor limité comme défaut global (max 2 assets parallèles)
    # Tous les jobs utilisent cet executor sauf override explicite
    executor=limited_executor,
)
