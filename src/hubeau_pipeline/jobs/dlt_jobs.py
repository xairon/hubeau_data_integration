from dagster import define_asset_job, AssetSelection, in_process_executor

from ..assets.raw.dlt_assets import (
    # Assets de stations de référence (pas de partition)
    hydrometry_stations_reference,
    hydrometry_sites_reference,
    piezometry_stations_reference,
    quality_rivers_stations_reference,
    quality_groundwater_stations_reference,
    ecoulement_stations_reference,
    hydrobio_stations_reference,
    prelevements_ouvrages_reference,
    prelevements_points_reference,
    temperature_stations_reference,
    
    # Assets d'observations/analyses (avec partitions annuelles)
    hydrobio_taxons,
    hydrobio_indices,
    hydrometry_obs_elab,
    piezometry_chroniques,
    quality_rivers_analyses,
    quality_groundwater_analyses,
    quality_rivers_operations,
    quality_rivers_conditions,
    piezometry_chroniques_historical,
    ecoulement_observations,
    prelevements_chroniques,
    temperature_chroniques,
)

# ====================================
# JOBS PAR API - CHAÎNAGE LOGIQUE
# ====================================

# 🌊 HYDROMÉTRIE : Stations + Sites → Observations Élaborées
hydrometry_job = define_asset_job(
    name="hubeau_hydrometry_job",
    selection=AssetSelection.assets(
        hydrometry_stations_reference,  # 1. Récupérer les stations
        hydrometry_sites_reference,     # 2. Récupérer les sites
        hydrometry_obs_elab            # 3. Récupérer observations élaborées (historique complet)
    ),
    description="Job Hydrométrie : Stations + Sites → Observations élaborées avec partitions annuelles",
    executor_def=in_process_executor,  # ✅ OPTIMISATION MÉMOIRE: Exécution in-process
)

# 🏔️ PIÉZOMÉTRIE : Stations → Chroniques
piezometry_job = define_asset_job(
    name="hubeau_piezometry_job",
    selection=AssetSelection.assets(
        piezometry_stations_reference,  # 1. Récupérer les stations BSS
        piezometry_chroniques          # 2. Récupérer les chroniques (dépend de stations)
    ),
    description="Job Piézométrie : Stations → Chroniques avec partitions annuelles",
    executor_def=in_process_executor,  # ✅ OPTIMISATION MÉMOIRE: Exécution in-process
)

# 🧪 QUALITÉ COURS D'EAU : Stations → Analyses
quality_rivers_job = define_asset_job(
    name="hubeau_quality_rivers_job",
    selection=AssetSelection.assets(
        quality_rivers_stations_reference,  # 1. Récupérer les stations
        quality_rivers_analyses            # 2. Récupérer les analyses (dépend de stations)
    ),
    description="Job Qualité Cours d'Eau : Stations → Analyses avec partitions annuelles",
    executor_def=in_process_executor,  # ✅ OPTIMISATION MÉMOIRE: Exécution in-process
)

# 🧪 QUALITÉ NAPPES : Stations → Analyses
quality_groundwater_job = define_asset_job(
    name="hubeau_quality_groundwater_job",
    selection=AssetSelection.assets(
        quality_groundwater_stations_reference,  # 1. Récupérer les stations BSS
        quality_groundwater_analyses            # 2. Récupérer les analyses (dépend de stations)
    ),
    description="Job Qualité Nappes : Stations → Analyses avec partitions annuelles",
    executor_def=in_process_executor,  # ✅ OPTIMISATION MÉMOIRE: Exécution in-process
)

# 🌊 ÉCOULEMENT : Stations → Observations
ecoulement_job = define_asset_job(
    name="hubeau_ecoulement_job",
    selection=AssetSelection.assets(
        ecoulement_stations_reference,  # 1. Récupérer les stations ONDE
        ecoulement_observations        # 2. Récupérer les observations (dépend de stations)
    ),
    description="Job Écoulement : Stations → Observations avec partitions annuelles",
    executor_def=in_process_executor,  # ✅ OPTIMISATION MÉMOIRE: Exécution in-process
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
    executor_def=in_process_executor,  # ✅ OPTIMISATION MÉMOIRE: Exécution in-process
)

# 💧 PRÉLÈVEMENTS : Ouvrages + Points → Chroniques
prelevements_job = define_asset_job(
    name="hubeau_prelevements_job",
    selection=AssetSelection.assets(
        prelevements_ouvrages_reference,  # 1. Récupérer les ouvrages de prélèvement
        prelevements_points_reference,    # 2. Récupérer les points de prélèvement
        prelevements_chroniques          # 3. Récupérer les chroniques (dépend des ouvrages)
    ),
    description="Job Prélèvements : Ouvrages + Points → Chroniques avec partitions annuelles",
    executor_def=in_process_executor,  # ✅ OPTIMISATION MÉMOIRE: Exécution in-process
)

# 🌡️ TEMPÉRATURE : Stations → Chroniques
temperature_job = define_asset_job(
    name="hubeau_temperature_job",
    selection=AssetSelection.assets(
        temperature_stations_reference,  # 1. Récupérer les stations
        temperature_chroniques          # 2. Récupérer les chroniques (dépend de stations)
    ),
    description="Job Température : Stations → Chroniques avec partitions annuelles",
    executor_def=in_process_executor,  # ✅ OPTIMISATION MÉMOIRE: Exécution in-process
)

# ====================================
# JOBS GLOBAUX POUR ORCHESTRATION
# ====================================

# Job pour TOUS les référentiels (stations) - exécution rapide
sync_all_stations = define_asset_job(
    name="sync_all_stations",
    selection=AssetSelection.assets(
        hydrometry_stations_reference,
        hydrometry_sites_reference,
        piezometry_stations_reference,
        quality_rivers_stations_reference,
        quality_groundwater_stations_reference,
        ecoulement_stations_reference,
        hydrobio_stations_reference,
        prelevements_ouvrages_reference,
        prelevements_points_reference,
        temperature_stations_reference,
    ),
    description="Job global : Tous les référentiels de stations/sites (pas de partition)",
    executor_def=in_process_executor,  # ✅ OPTIMISATION MÉMOIRE: Exécution in-process
)

# Job pour les données avec partitions ANNUELLES uniquement (TOUTES les APIs)
sync_all_yearly_data = define_asset_job(
    name="sync_all_yearly_data",
    selection=AssetSelection.assets(
        # Stations/Sites de référence d'abord
        hydrometry_stations_reference,
        hydrometry_sites_reference,
        piezometry_stations_reference,
        quality_rivers_stations_reference,
        quality_groundwater_stations_reference,
        ecoulement_stations_reference,
        hydrobio_stations_reference,
        prelevements_ouvrages_reference,
        prelevements_points_reference,
        temperature_stations_reference,
        # Puis observations/analyses avec partitions annuelles
        hydrometry_obs_elab,
        hydrobio_taxons,
        hydrobio_indices,
        piezometry_chroniques,
        piezometry_chroniques_historical,
        quality_rivers_analyses,
        quality_rivers_operations,
        quality_rivers_conditions,
        quality_groundwater_analyses,
        ecoulement_observations,
        prelevements_chroniques,
        temperature_chroniques,
    ),
    description="Job global : TOUTES les 8 APIs avec partitions annuelles (hydrométrie, piézo, qualité cours d'eau, qualité nappes, écoulement, hydrobio, prélèvements, température)",
    executor_def=in_process_executor,  # ✅ OPTIMISATION MÉMOIRE: Exécution in-process
)

# ====================================
# JOBS GLOBAUX INTELLIGENTS (OBSERVATIONS)
# ====================================

# Job intelligent: Charge automatiquement les années manquantes
# Utilise get_partitions_to_load() pour déterminer quelles années charger
sync_all_observations_auto = define_asset_job(
    name="sync_all_observations_auto",
    selection=AssetSelection.assets(
        # Observations/analyses avec partitions annuelles
        hydrometry_obs_elab,
        hydrobio_taxons,
        hydrobio_indices,
        piezometry_chroniques,
        quality_rivers_analyses,
        quality_rivers_operations,
        quality_rivers_conditions,
        quality_groundwater_analyses,
        ecoulement_observations,
        prelevements_chroniques,
        temperature_chroniques,
    ),
    description="""Job intelligent: Charge automatiquement les observations manquantes

    Pour chaque asset:
    - Si table vide → charge depuis 2010
    - Si table existe → charge depuis (max_date + 1 jour)

    Exemple: Si max_date=2023-06-15 aujourd'hui=2024-10-22
    → Charge partitions 2023 et 2024

    Idéal pour: Scheduling quotidien/hebdomadaire automatique
    """,
    executor_def=in_process_executor,
)

# Job manuel: Charge les années spécifiées par l'utilisateur
sync_all_observations_by_year = define_asset_job(
    name="sync_all_observations_by_year",
    selection=AssetSelection.assets(
        # Observations/analyses avec partitions annuelles
        hydrometry_obs_elab,
        hydrobio_taxons,
        hydrobio_indices,
        piezometry_chroniques,
        quality_rivers_analyses,
        quality_rivers_operations,
        quality_rivers_conditions,
        quality_groundwater_analyses,
        ecoulement_observations,
        prelevements_chroniques,
        temperature_chroniques,
    ),
    description="""Job manuel: Charge les années spécifiées

    Utilisation:
    - Dans Dagster UI: Sélectionner les partitions (années) manuellement
    - CLI: dagster job execute -j sync_all_observations_by_year -p '2023'
    - CLI multi-années: -p '2020,2021,2022,2023'

    Idéal pour: Backfill manuel, rechargement d'années spécifiques
    """,
    executor_def=in_process_executor,
)

# ✅ SIMPLIFIÉ: Tous les jobs utilisent maintenant des partitions annuelles
# sync_all_daily_data et sync_realtime_data supprimés car observations_tr retiré