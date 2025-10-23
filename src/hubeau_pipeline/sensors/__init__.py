"""
Sensors - Monitoring et alertes
Surveillance des pipelines et détection d'anomalies
"""

from .error_detection import error_detection_sensor
# Disabled for CSV architecture (no partitions)
# from .backfill_sensor import backfill_missing_partitions_sensor
from .failure_sensor import (
    pipeline_failure_alert_sensor,
    long_running_pipeline_sensor,
    repeated_failure_sensor,
)

all_sensors = [
    # Sensors actifs par défaut
    error_detection_sensor,
    # backfill_missing_partitions_sensor,  # Disabled for CSV architecture
    pipeline_failure_alert_sensor,

    # Sensors optionnels (STOPPED par défaut)
    long_running_pipeline_sensor,
    repeated_failure_sensor,
]

__all__ = [
    "all_sensors",
    "error_detection_sensor",
    # "backfill_missing_partitions_sensor",  # Disabled for CSV architecture
    "pipeline_failure_alert_sensor",
    "long_running_pipeline_sensor",
    "repeated_failure_sensor",
]
