"""
Jobs Hub'Eau améliorés avec chaînage complet

Chaque job inclut :
1. Les assets CSV d'ingestion
2. Les assets _schema_optimized correspondants
3. L'asset de monitoring basic_database_check

Ordre d'exécution :
CSV → Schema Optimization → Monitoring
"""

from dagster import define_asset_job, AssetSelection, AssetKey

# ====================================
# CONFIGURATION GLOBALE - EXECUTION LIMITÉE
# ====================================
# Limite le parallélisme pour économiser la RAM
EXECUTOR_CONFIG = {
    "execution": {
        "config": {
            "multiprocess": {
                "max_concurrent": 2  # 2 assets max en parallèle
            }
        }
    }
}

# ====================================
# JOBS PAR API AVEC SCHEMA OPTIMIZATION
# ====================================

# PIEZOMETRY: Pipeline complet avec optimisation
piezometry_complete_job = define_asset_job(
    name="piezometry_complete",
    description="Piézométrie: CSV → Schema Optimization → Monitoring",
    selection=AssetSelection.keys(
        # 1. CSV Data
        AssetKey("piezometry_stations_csv"),
        AssetKey("piezometry_chroniques_csv"),
        # 2. Schema Optimization
        AssetKey("piezometry_stations_schema_optimized"),
        AssetKey("piezometry_chroniques_schema_optimized"),
        # 3. Monitoring
        AssetKey("basic_database_check")
    ),
    config=EXECUTOR_CONFIG
)

# QUALITY RIVERS: Pipeline complet avec optimisation
quality_rivers_complete_job = define_asset_job(
    name="quality_rivers_complete",
    description="Qualité Rivières: CSV → Schema Optimization → Monitoring",
    selection=AssetSelection.keys(
        # 1. CSV Data
        AssetKey("quality_rivers_stations_csv"),
        AssetKey("quality_rivers_analyses_csv"),
        AssetKey("quality_rivers_conditions_csv"),
        AssetKey("quality_rivers_operations_csv"),
        # 2. Schema Optimization
        AssetKey("quality_rivers_stations_schema_optimized"),
        AssetKey("quality_rivers_analyses_schema_optimized"),
        AssetKey("quality_rivers_conditions_schema_optimized"),
        AssetKey("quality_rivers_operations_schema_optimized"),
        # 3. Monitoring
        AssetKey("basic_database_check")
    ),
    config=EXECUTOR_CONFIG
)

# QUALITY GROUNDWATER: Pipeline complet avec optimisation
quality_groundwater_complete_job = define_asset_job(
    name="quality_groundwater_complete",
    description="Qualité Nappes: CSV → Schema Optimization → Monitoring",
    selection=AssetSelection.keys(
        # 1. CSV Data
        AssetKey("quality_groundwater_stations_csv"),
        AssetKey("quality_groundwater_analyses_csv"),
        # 2. Schema Optimization
        AssetKey("quality_groundwater_stations_schema_optimized"),
        AssetKey("quality_groundwater_analyses_schema_optimized"),
        # 3. Monitoring
        AssetKey("basic_database_check")
    ),
    config=EXECUTOR_CONFIG
)

# HYDROMETRY: Pipeline complet avec optimisation
hydrometry_complete_job = define_asset_job(
    name="hydrometry_complete",
    description="Hydrométrie: CSV → Schema Optimization → Monitoring",
    selection=AssetSelection.keys(
        # 1. CSV Data
        AssetKey("hydrometry_sites_csv"),
        AssetKey("hydrometry_stations_csv"),
        AssetKey("hydrometry_obs_elab_csv"),
        # 2. Schema Optimization
        AssetKey("hydrometry_sites_schema_optimized"),
        AssetKey("hydrometry_stations_schema_optimized"),
        AssetKey("hydrometry_obs_elab_schema_optimized"),
        # 3. Monitoring
        AssetKey("basic_database_check")
    ),
    config=EXECUTOR_CONFIG
)

# TEMPERATURE: Pipeline complet avec optimisation
temperature_complete_job = define_asset_job(
    name="temperature_complete",
    description="Température: CSV → Schema Optimization → Monitoring",
    selection=AssetSelection.keys(
        # 1. CSV Data
        AssetKey("temperature_stations_csv"),
        AssetKey("temperature_chroniques_csv"),
        # 2. Schema Optimization
        AssetKey("temperature_stations_schema_optimized"),
        AssetKey("temperature_chroniques_schema_optimized"),
        # 3. Monitoring
        AssetKey("basic_database_check")
    ),
    config=EXECUTOR_CONFIG
)

# HYDROBIO: Pipeline complet avec optimisation
hydrobio_complete_job = define_asset_job(
    name="hydrobio_complete",
    description="Hydrobiologie: CSV → Schema Optimization → Monitoring",
    selection=AssetSelection.keys(
        # 1. CSV Data
        AssetKey("hydrobio_stations_csv"),
        AssetKey("hydrobio_indices_csv"),
        AssetKey("hydrobio_taxons_csv"),
        # 2. Schema Optimization
        AssetKey("hydrobio_stations_schema_optimized"),
        AssetKey("hydrobio_indices_schema_optimized"),
        AssetKey("hydrobio_taxons_schema_optimized"),
        # 3. Monitoring
        AssetKey("basic_database_check")
    ),
    config=EXECUTOR_CONFIG
)

# ECOULEMENT: Pipeline complet avec optimisation
ecoulement_complete_job = define_asset_job(
    name="ecoulement_complete",
    description="Écoulement: CSV → Schema Optimization → Monitoring",
    selection=AssetSelection.keys(
        # 1. CSV Data
        AssetKey("ecoulement_stations_csv"),
        AssetKey("ecoulement_campagnes_csv"),
        AssetKey("ecoulement_observations_csv"),
        # 2. Schema Optimization
        AssetKey("ecoulement_stations_schema_optimized"),
        AssetKey("ecoulement_campagnes_schema_optimized"),
        AssetKey("ecoulement_observations_schema_optimized"),
        # 3. Monitoring
        AssetKey("basic_database_check")
    ),
    config=EXECUTOR_CONFIG
)

# PRELEVEMENTS: Pipeline complet avec optimisation
prelevements_complete_job = define_asset_job(
    name="prelevements_complete",
    description="Prélèvements: CSV → Schema Optimization → Monitoring",
    selection=AssetSelection.keys(
        # 1. CSV Data
        AssetKey("prelevements_ouvrages_csv"),
        AssetKey("prelevements_points_csv"),
        AssetKey("prelevements_chroniques_csv"),
        # 2. Schema Optimization
        AssetKey("prelevements_ouvrages_schema_optimized"),
        AssetKey("prelevements_points_schema_optimized"),
        AssetKey("prelevements_chroniques_schema_optimized"),
        # 3. Monitoring
        AssetKey("basic_database_check")
    ),
    config=EXECUTOR_CONFIG
)

# ====================================
# JOBS GLOBAUX AVEC SCHEMA OPTIMIZATION
# ====================================

# TOUTES les stations avec optimisation
all_stations_complete_job = define_asset_job(
    name="all_stations_complete",
    description="Toutes les stations: CSV → Schema Optimization → Monitoring",
    selection=AssetSelection.keys(
        # 1. Tous les CSV stations
        AssetKey("piezometry_stations_csv"),
        AssetKey("quality_rivers_stations_csv"),
        AssetKey("quality_groundwater_stations_csv"),
        AssetKey("hydrometry_sites_csv"),
        AssetKey("hydrometry_stations_csv"),
        AssetKey("temperature_stations_csv"),
        AssetKey("hydrobio_stations_csv"),
        AssetKey("ecoulement_stations_csv"),
        AssetKey("ecoulement_campagnes_csv"),
        AssetKey("prelevements_ouvrages_csv"),
        AssetKey("prelevements_points_csv"),
        # 2. Toutes les optimisations stations
        AssetKey("piezometry_stations_schema_optimized"),
        AssetKey("quality_rivers_stations_schema_optimized"),
        AssetKey("quality_groundwater_stations_schema_optimized"),
        AssetKey("hydrometry_sites_schema_optimized"),
        AssetKey("hydrometry_stations_schema_optimized"),
        AssetKey("temperature_stations_schema_optimized"),
        AssetKey("hydrobio_stations_schema_optimized"),
        AssetKey("ecoulement_stations_schema_optimized"),
        AssetKey("ecoulement_campagnes_schema_optimized"),
        AssetKey("prelevements_ouvrages_schema_optimized"),
        AssetKey("prelevements_points_schema_optimized"),
        # 3. Monitoring final
        AssetKey("basic_database_check")
    ),
    config=EXECUTOR_CONFIG
)

# TOUTES les chroniques avec optimisation
all_chroniques_complete_job = define_asset_job(
    name="all_chroniques_complete",
    description="Toutes les chroniques: CSV → Schema Optimization → Monitoring",
    selection=AssetSelection.keys(
        # 1. Tous les CSV chroniques
        AssetKey("piezometry_chroniques_csv"),
        AssetKey("quality_rivers_analyses_csv"),
        AssetKey("quality_rivers_conditions_csv"),
        AssetKey("quality_rivers_operations_csv"),
        AssetKey("quality_groundwater_analyses_csv"),
        AssetKey("hydrometry_obs_elab_csv"),
        AssetKey("temperature_chroniques_csv"),
        AssetKey("hydrobio_indices_csv"),
        AssetKey("hydrobio_taxons_csv"),
        AssetKey("ecoulement_observations_csv"),
        AssetKey("prelevements_chroniques_csv"),
        # 2. Toutes les optimisations chroniques
        AssetKey("piezometry_chroniques_schema_optimized"),
        AssetKey("quality_rivers_analyses_schema_optimized"),
        AssetKey("quality_rivers_conditions_schema_optimized"),
        AssetKey("quality_rivers_operations_schema_optimized"),
        AssetKey("quality_groundwater_analyses_schema_optimized"),
        AssetKey("hydrometry_obs_elab_schema_optimized"),
        AssetKey("temperature_chroniques_schema_optimized"),
        AssetKey("hydrobio_indices_schema_optimized"),
        AssetKey("hydrobio_taxons_schema_optimized"),
        AssetKey("ecoulement_observations_schema_optimized"),
        AssetKey("prelevements_chroniques_schema_optimized"),
        # 3. Monitoring final
        AssetKey("basic_database_check")
    ),
    config=EXECUTOR_CONFIG
)

# PIPELINE COMPLET (tout + optimisations + monitoring)
full_pipeline_job = define_asset_job(
    name="full_pipeline",
    description="Pipeline complet: Toutes les données → Optimisations → Monitoring",
    selection=AssetSelection.all(),  # Tous les assets y compris schema et monitoring
    config=EXECUTOR_CONFIG
)

# ====================================
# JOBS RAPIDES (CSV uniquement, sans optimisation)
# ====================================
# Pour tests rapides ou refresh de données uniquement

quick_stations_job = define_asset_job(
    name="quick_stations",
    description="Stations CSV uniquement (sans optimisation)",
    selection=AssetSelection.keys(
        AssetKey("piezometry_stations_csv"),
        AssetKey("quality_rivers_stations_csv"),
        AssetKey("quality_groundwater_stations_csv"),
        AssetKey("hydrometry_sites_csv"),
        AssetKey("hydrometry_stations_csv"),
        AssetKey("temperature_stations_csv"),
        AssetKey("hydrobio_stations_csv"),
        AssetKey("ecoulement_stations_csv"),
        AssetKey("ecoulement_campagnes_csv"),
        AssetKey("prelevements_ouvrages_csv"),
        AssetKey("prelevements_points_csv")
    ),
    config=EXECUTOR_CONFIG
)

quick_chroniques_job = define_asset_job(
    name="quick_chroniques",
    description="Chroniques CSV uniquement (sans optimisation)",
    selection=AssetSelection.keys(
        AssetKey("piezometry_chroniques_csv"),
        AssetKey("quality_rivers_analyses_csv"),
        AssetKey("quality_rivers_conditions_csv"),
        AssetKey("quality_rivers_operations_csv"),
        AssetKey("quality_groundwater_analyses_csv"),
        AssetKey("hydrometry_obs_elab_csv"),
        AssetKey("temperature_chroniques_csv"),
        AssetKey("hydrobio_indices_csv"),
        AssetKey("hydrobio_taxons_csv"),
        AssetKey("ecoulement_observations_csv"),
        AssetKey("prelevements_chroniques_csv")
    ),
    config=EXECUTOR_CONFIG
)

# ====================================
# JOBS D'OPTIMISATION SEULS
# ====================================
# Pour optimiser après ingestion de données

schema_optimization_only_job = define_asset_job(
    name="schema_optimization_only",
    description="Optimisation de schéma uniquement (nécessite données existantes)",
    selection=AssetSelection.keys(
        # Toutes les optimisations
        AssetKey("piezometry_stations_schema_optimized"),
        AssetKey("piezometry_chroniques_schema_optimized"),
        AssetKey("quality_rivers_stations_schema_optimized"),
        AssetKey("quality_rivers_analyses_schema_optimized"),
        AssetKey("quality_rivers_conditions_schema_optimized"),
        AssetKey("quality_rivers_operations_schema_optimized"),
        AssetKey("quality_groundwater_stations_schema_optimized"),
        AssetKey("quality_groundwater_analyses_schema_optimized"),
        AssetKey("hydrometry_sites_schema_optimized"),
        AssetKey("hydrometry_stations_schema_optimized"),
        AssetKey("hydrometry_obs_elab_schema_optimized"),
        AssetKey("temperature_stations_schema_optimized"),
        AssetKey("temperature_chroniques_schema_optimized"),
        AssetKey("hydrobio_stations_schema_optimized"),
        AssetKey("hydrobio_indices_schema_optimized"),
        AssetKey("hydrobio_taxons_schema_optimized"),
        AssetKey("ecoulement_stations_schema_optimized"),
        AssetKey("ecoulement_campagnes_schema_optimized"),
        AssetKey("ecoulement_observations_schema_optimized"),
        AssetKey("prelevements_ouvrages_schema_optimized"),
        AssetKey("prelevements_points_schema_optimized"),
        AssetKey("prelevements_chroniques_schema_optimized"),
        # Monitoring final
        AssetKey("basic_database_check")
    ),
    config=EXECUTOR_CONFIG
)

# Export des jobs
all_improved_jobs = [
    # Jobs complets par API
    piezometry_complete_job,
    quality_rivers_complete_job,
    quality_groundwater_complete_job,
    hydrometry_complete_job,
    temperature_complete_job,
    hydrobio_complete_job,
    ecoulement_complete_job,
    prelevements_complete_job,
    # Jobs globaux complets
    all_stations_complete_job,
    all_chroniques_complete_job,
    full_pipeline_job,
    # Jobs rapides (CSV uniquement)
    quick_stations_job,
    quick_chroniques_job,
    # Job d'optimisation seul
    schema_optimization_only_job,
]