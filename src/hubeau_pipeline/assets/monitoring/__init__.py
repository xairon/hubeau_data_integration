"""
Assets de monitoring pour Hub'Eau pipeline
"""

from .data_quality import (
    piezometry_data_quality,
    quality_rivers_data_quality,
    hydrometry_data_quality,
    quality_groundwater_data_quality,
    ecoulement_data_quality,
    hydrobio_data_quality,
    prelevements_data_quality,
    temperature_data_quality,
    global_data_quality_report
)

from .performance_tracker import (
    system_performance_metrics,
    database_performance_metrics,
    dlt_pipeline_metrics,
    performance_alert_sensor
)

from .dashboard import (
    hubeau_monitoring_dashboard,
    executive_summary_report
)

all_monitoring_assets = [
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
]

__all__ = [
    "all_monitoring_assets",
    "performance_alert_sensor"
]
