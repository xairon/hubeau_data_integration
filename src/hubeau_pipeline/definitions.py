"""
Définitions Dagster centrales - Point d'entrée de l'application
"""

import os
from dagster import Definitions, define_asset_job, AssetSelection, multiprocess_executor

# Import des assets
from .assets import all_assets

# Import des jobs
from .jobs import all_jobs

# Import des schedules (vide actuellement - pour extensions futures)
from .schedules import all_schedules

# Import des sensors (CSV auto-ingestion)
from .sensors import all_sensors

# Import des resources
from .resources import RESOURCES
from .io.io_managers import noop_io_manager

# ============================================================================
# EXECUTOR CONFIG - Limite parallélisme pour économiser RAM
# ============================================================================
# Limite à 3 assets en parallèle (optimisé avec rate_limit 0.3s)
# Impact : Balance performance/RAM (évite SIGKILL / OOM)
# ============================================================================

# ✅ CHANGEMENT #4: Executor limité PAR DÉFAUT pour TOUS les runs
# Configuration globale de l'executor pour limiter le parallélisme
limited_executor = multiprocess_executor.configured({
    "max_concurrent": 3  # ✅ Max 3 assets en parallèle (optimisé avec rate_limit 0.3s)
})

# Définitions centrales
defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,  # 18 jobs: 8 stations + 8 chroniques + 2 globaux (Bronze layer)
    schedules=all_schedules,  # Empty - schedules removed, manual materialization only
    sensors=all_sensors,  # CSV auto-ingestion sensors
    resources={
        **RESOURCES,  # Database connections, etc.
        "noop_io_manager": noop_io_manager,  # For DLT assets that write to PostgreSQL
    },
    # Executor limité comme défaut global (max 3 assets parallèles)
    # Tous les jobs utilisent cet executor sauf override explicite
    executor=limited_executor,
)
