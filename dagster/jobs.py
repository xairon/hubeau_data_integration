"""Dagster job definitions for the dlt ingestion assets."""
from __future__ import annotations

from dagster import ScheduleDefinition, define_asset_job

from dagster.assets.dlt_assets import (
    hydrobio_taxons,
    hydrobio_indices,
    hydrometry_observations,
    piezometry_chroniques,
    quality_rivers_analyses,
    quality_groundwater_analyses,
    ecoulement_observations,
    prelevements_chroniques,
    temperature_chroniques,
)


# ====================================
# JOBS PAR API
# ====================================

# Job Hydrobiologie
hydrobio_job = define_asset_job(
    name="hubeau_hydrobio_job",
    selection=[hydrobio_taxons.key, hydrobio_indices.key],
    description="🐟 Ingestion Hydrobiologie (taxons + indices)",
)

# Job Hydrométrie
hydrometry_job = define_asset_job(
    name="hubeau_hydrometry_job",
    selection=[hydrometry_observations.key],
    description="🌊 Ingestion Hydrométrie (30 derniers jours)",
)

# Job Piézométrie
piezometry_job = define_asset_job(
    name="hubeau_piezometry_job",
    selection=[piezometry_chroniques.key],
    description="🕳️ Ingestion Piézométrie (chroniques)",
)

# Job Qualité
quality_job = define_asset_job(
    name="hubeau_quality_job",
    selection=[quality_rivers_analyses.key, quality_groundwater_analyses.key],
    description="🏞️💧 Ingestion Qualité (cours d'eau + nappes)",
)

# Job Écoulement
ecoulement_job = define_asset_job(
    name="hubeau_ecoulement_job",
    selection=[ecoulement_observations.key],
    description="🌊 Ingestion Écoulement ONDE",
)

# Job Prélèvements
prelevements_job = define_asset_job(
    name="hubeau_prelevements_job",
    selection=[prelevements_chroniques.key],
    description="💧 Ingestion Prélèvements (limite 20k)",
)

# Job Température
temperature_job = define_asset_job(
    name="hubeau_temperature_job",
    selection=[temperature_chroniques.key],
    description="🌡️ Ingestion Température (station×mois)",
)

# ====================================
# JOBS GLOBAUX
# ====================================

# Job quotidien (toutes les APIs)
sync_hubeau_daily = define_asset_job(
    name="sync_hubeau_daily",
    selection=[
        hydrobio_taxons.key,
        hydrobio_indices.key,
        hydrometry_observations.key,
        piezometry_chroniques.key,
        quality_rivers_analyses.key,
        quality_groundwater_analyses.key,
        ecoulement_observations.key,
        prelevements_chroniques.key,
        temperature_chroniques.key,
    ],
    description="🚀 Ingestion complète Hub'Eau (toutes les APIs)",
)

# Job rapide (APIs temps réel)
sync_hubeau_realtime = define_asset_job(
    name="sync_hubeau_realtime",
    selection=[
        hydrometry_observations.key,
        piezometry_chroniques.key,
    ],
    description="⚡ Ingestion temps réel (hydrométrie + piézométrie)",
)

# Job qualité (APIs qualité)
sync_hubeau_quality = define_asset_job(
    name="sync_hubeau_quality",
    selection=[
        quality_rivers_analyses.key,
        quality_groundwater_analyses.key,
    ],
    description="🧪 Ingestion qualité des eaux",
)

# ====================================
# SCHEDULES
# ====================================

# Schedule quotidien (4h du matin)
sync_hubeau_daily_schedule = ScheduleDefinition(
    job=sync_hubeau_daily,
    cron_schedule="0 4 * * *",  # Tous les jours à 4h
)

# Schedule temps réel (toutes les heures)
sync_hubeau_realtime_schedule = ScheduleDefinition(
    job=sync_hubeau_realtime,
    cron_schedule="0 * * * *",  # Toutes les heures
)

# Schedule qualité (hebdomadaire, dimanche à 2h)
sync_hubeau_quality_schedule = ScheduleDefinition(
    job=sync_hubeau_quality,
    cron_schedule="0 2 * * 0",  # Tous les dimanches à 2h
)

# ====================================
# EXPORT
# ====================================

__all__ = [
    # Jobs par API
    "hydrobio_job",
    "hydrometry_job",
    "piezometry_job",
    "quality_job",
    "ecoulement_job",
    "prelevements_job",
    "temperature_job",
    # Jobs globaux
    "sync_hubeau_daily",
    "sync_hubeau_realtime",
    "sync_hubeau_quality",
    # Schedules
    "sync_hubeau_daily_schedule",
    "sync_hubeau_realtime_schedule",
    "sync_hubeau_quality_schedule",
]
