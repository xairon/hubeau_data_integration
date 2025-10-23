"""
Jobs Dagster - Orchestration des pipelines d'ingestion
"""

# Jobs CSV (nouvelle architecture - recommended)
from .csv_backfill_jobs import (
    backfill_temperature_job,
    backfill_piezometry_job,
    backfill_quality_rivers_job,
    backfill_hydrometry_job,
    backfill_all_job,
)

from .csv_incremental_jobs import (
    incremental_chroniques_job,
    incremental_references_job,
)

# Jobs JSON dlt (ancienne architecture - deprecated)
# from .dlt_jobs import (
#     hydrobio_job,
#     hydrometry_job,
#     piezometry_job,
#     quality_rivers_job,
#     quality_groundwater_job,
#     ecoulement_job,
#     prelevements_job,
#     temperature_job,
#     sync_all_stations,
#     sync_all_yearly_data,
#     sync_all_observations_auto,
#     sync_all_observations_by_year,
# )

# Jobs CSV (nouvelle architecture)
csv_jobs = [
    # Backfill
    backfill_temperature_job,
    backfill_piezometry_job,
    backfill_quality_rivers_job,
    backfill_hydrometry_job,
    backfill_all_job,
    # Incremental
    incremental_chroniques_job,
    incremental_references_job,
]

# ✅ NOUVELLE ARCHITECTURE CSV: Utiliser les jobs CSV par défaut
all_jobs = csv_jobs

__all__ = [
    # Jobs CSV (nouvelle architecture)
    "csv_jobs",
    "backfill_temperature_job",
    "backfill_piezometry_job",
    "backfill_quality_rivers_job",
    "backfill_hydrometry_job",
    "backfill_all_job",
    "incremental_chroniques_job",
    "incremental_references_job",

    # Tous les jobs
    "all_jobs"
]
