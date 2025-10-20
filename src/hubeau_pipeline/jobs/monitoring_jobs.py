"""
Jobs de monitoring Hub'Eau
"""

from dagster import define_asset_job, AssetSelection, in_process_executor

from ..assets.monitoring import (
    # Qualité des données
    piezometry_data_quality,
    quality_rivers_data_quality,
    hydrometry_data_quality,
    quality_groundwater_data_quality,
    ecoulement_data_quality,
    hydrobio_data_quality,
    prelevements_data_quality,
    temperature_data_quality,
    global_data_quality_report,
    
    # Performance
    system_performance_metrics,
    database_performance_metrics,
    dlt_pipeline_metrics,
    
    # Dashboard
    hubeau_monitoring_dashboard,
    executive_summary_report
)

# ====================================
# JOBS DE MONITORING
# ====================================

# Job de monitoring de la qualité des données
data_quality_monitoring_job = define_asset_job(
    name="data_quality_monitoring",
    selection=AssetSelection.assets(
        # Toutes les validations de qualité par API
        piezometry_data_quality,
        quality_rivers_data_quality,
        hydrometry_data_quality,
        quality_groundwater_data_quality,
        ecoulement_data_quality,
        hydrobio_data_quality,
        prelevements_data_quality,
        temperature_data_quality,
        global_data_quality_report,
    ),
    description="Monitoring complet de la qualité des données pour toutes les APIs Hub'Eau",
    executor_def=in_process_executor,
)

# Job de monitoring des performances
performance_monitoring_job = define_asset_job(
    name="performance_monitoring",
    selection=AssetSelection.assets(
        # Métriques de performance
        system_performance_metrics,
        database_performance_metrics,
        dlt_pipeline_metrics,
    ),
    description="Monitoring des performances système, base de données et pipelines DLT",
    executor_def=in_process_executor,
)

# Job de dashboard complet
full_monitoring_job = define_asset_job(
    name="full_monitoring_dashboard",
    selection=AssetSelection.assets(
        # Toutes les métriques
        piezometry_data_quality,
        quality_rivers_data_quality,
        hydrometry_data_quality,
        quality_groundwater_data_quality,
        ecoulement_data_quality,
        hydrobio_data_quality,
        prelevements_data_quality,
        temperature_data_quality,
        global_data_quality_report,
        system_performance_metrics,
        database_performance_metrics,
        dlt_pipeline_metrics,
        hubeau_monitoring_dashboard,
        executive_summary_report,
    ),
    description="Dashboard complet de monitoring Hub'Eau avec toutes les métriques",
    executor_def=in_process_executor,
)

# Job de monitoring rapide (métriques essentielles)
quick_monitoring_job = define_asset_job(
    name="quick_monitoring",
    selection=AssetSelection.assets(
        # Métriques essentielles seulement
        global_data_quality_report,
        system_performance_metrics,
        dlt_pipeline_metrics,
        hubeau_monitoring_dashboard,
    ),
    description="Monitoring rapide avec les métriques essentielles",
    executor_def=in_process_executor,
)
