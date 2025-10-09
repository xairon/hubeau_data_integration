from dagster import define_asset_job, AssetSelection
from ..assets.bronze.dlt_assets import (
    # Assets de stations de référence (pas de partition)
    hydrometry_stations_reference,
    piezometry_stations_reference,
    quality_rivers_stations_reference,
    quality_groundwater_stations_reference,
    ecoulement_stations_reference,
    hydrobio_stations_reference,
    prelevements_stations_reference,
    temperature_stations_reference,
    
    # Assets d'observations/analyses (avec partitions)
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
# JOBS PAR API - CHAÎNAGE LOGIQUE
# ====================================

# 🌊 HYDROMÉTRIE : Stations → Observations
hydrometry_job = define_asset_job(
    name="hubeau_hydrometry_job",
    selection=AssetSelection.assets(
        hydrometry_stations_reference,  # 1. Récupérer les stations
        hydrometry_observations         # 2. Récupérer les observations (dépend de stations)
    ),
    description="Job Hydrométrie : Stations → Observations temps réel (30j max)",
)

# 🏔️ PIÉZOMÉTRIE : Stations → Chroniques
piezometry_job = define_asset_job(
    name="hubeau_piezometry_job",
    selection=AssetSelection.assets(
        piezometry_stations_reference,  # 1. Récupérer les stations BSS
        piezometry_chroniques          # 2. Récupérer les chroniques (dépend de stations)
    ),
    description="Job Piézométrie : Stations → Chroniques avec partitions annuelles",
)

# 🧪 QUALITÉ COURS D'EAU : Stations → Analyses
quality_rivers_job = define_asset_job(
    name="hubeau_quality_rivers_job",
    selection=AssetSelection.assets(
        quality_rivers_stations_reference,  # 1. Récupérer les stations
        quality_rivers_analyses            # 2. Récupérer les analyses (dépend de stations)
    ),
    description="Job Qualité Cours d'Eau : Stations → Analyses avec partitions annuelles",
)

# 🧪 QUALITÉ NAPPES : Stations → Analyses
quality_groundwater_job = define_asset_job(
    name="hubeau_quality_groundwater_job",
    selection=AssetSelection.assets(
        quality_groundwater_stations_reference,  # 1. Récupérer les stations BSS
        quality_groundwater_analyses            # 2. Récupérer les analyses (dépend de stations)
    ),
    description="Job Qualité Nappes : Stations → Analyses avec partitions annuelles",
)

# 🌊 ÉCOULEMENT : Stations → Observations
ecoulement_job = define_asset_job(
    name="hubeau_ecoulement_job",
    selection=AssetSelection.assets(
        ecoulement_stations_reference,  # 1. Récupérer les stations ONDE
        ecoulement_observations        # 2. Récupérer les observations (dépend de stations)
    ),
    description="Job Écoulement : Stations → Observations avec partitions annuelles",
)

# 🐟 HYDROBIOLOGIE : Stations → Taxons + Indices
hydrobio_job = define_asset_job(
    name="hubeau_hydrobio_job",
    selection=AssetSelection.assets(
        hydrobio_stations_reference,  # 1. Récupérer les stations
        hydrobio_taxons,             # 2. Récupérer les taxons (dépend de stations)
        hydrobio_indices             # 3. Récupérer les indices (dépend de stations)
    ),
    description="Job Hydrobiologie : Stations → Taxons + Indices avec partitions annuelles",
)

# 💧 PRÉLÈVEMENTS : Stations → Chroniques
prelevements_job = define_asset_job(
    name="hubeau_prelevements_job",
    selection=AssetSelection.assets(
        prelevements_stations_reference,  # 1. Récupérer les points de prélèvement
        prelevements_chroniques          # 2. Récupérer les chroniques (dépend de stations)
    ),
    description="Job Prélèvements : Stations → Chroniques avec partitions annuelles",
)

# 🌡️ TEMPÉRATURE : Stations → Chroniques
temperature_job = define_asset_job(
    name="hubeau_temperature_job",
    selection=AssetSelection.assets(
        temperature_stations_reference,  # 1. Récupérer les stations
        temperature_chroniques          # 2. Récupérer les chroniques (dépend de stations)
    ),
    description="Job Température : Stations → Chroniques avec partitions annuelles",
)

# ====================================
# JOBS GLOBAUX POUR ORCHESTRATION
# ====================================

# Job pour TOUS les référentiels (stations) - exécution rapide
sync_all_stations = define_asset_job(
    name="sync_all_stations",
    selection=AssetSelection.assets(
        hydrometry_stations_reference,
        piezometry_stations_reference,
        quality_rivers_stations_reference,
        quality_groundwater_stations_reference,
        ecoulement_stations_reference,
        hydrobio_stations_reference,
        prelevements_stations_reference,
        temperature_stations_reference,
    ),
    description="Job global : Tous les référentiels de stations (pas de partition)",
)

# Job pour les données avec partitions ANNUELLES uniquement (7 APIs)
sync_all_yearly_data = define_asset_job(
    name="sync_all_yearly_data",
    selection=AssetSelection.assets(
        # Stations d'abord
        piezometry_stations_reference,
        quality_rivers_stations_reference,
        quality_groundwater_stations_reference,
        ecoulement_stations_reference,
        hydrobio_stations_reference,
        prelevements_stations_reference,
        temperature_stations_reference,
        # Puis observations/analyses avec partitions annuelles
        hydrobio_taxons,
        hydrobio_indices,
        piezometry_chroniques,
        quality_rivers_analyses,
        quality_groundwater_analyses,
        ecoulement_observations,
        prelevements_chroniques,
        temperature_chroniques,
    ),
    description="Job global : 7 APIs avec partitions annuelles (piézo, qualité cours d'eau, qualité nappes, écoulement, hydrobio, prélèvements, température)",
)

# Job pour les données avec partitions QUOTIDIENNES uniquement
sync_all_daily_data = define_asset_job(
    name="sync_all_daily_data",
    selection=AssetSelection.assets(
        # Stations d'abord
        hydrometry_stations_reference,
        # Puis observations avec partitions quotidiennes
        hydrometry_observations,
    ),
    description="Job global : Données avec fenêtre glissante quotidienne uniquement (hydrométrie temps réel 30j)",
)

# Job pour les données temps réel uniquement
sync_realtime_data = define_asset_job(
    name="sync_realtime_data",
    selection=AssetSelection.assets(
        hydrometry_stations_reference,  # Stations hydrométrie
        hydrometry_observations        # Observations temps réel (30j max)
    ),
    description="Job temps réel : Hydrométrie uniquement (30 derniers jours)",
)