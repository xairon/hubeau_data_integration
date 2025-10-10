"""
Sensors - Monitoring et alertes
Surveillance des pipelines et détection d'anomalies
"""

# NOTE: hubeau_freshness_sensor disabled - sync_realtime_data job was removed
# from .data_freshness import hubeau_freshness_sensor
from .error_detection import error_detection_sensor

all_sensors = [
    # hubeau_freshness_sensor,  # Disabled - realtime data migrated to yearly partitions
    error_detection_sensor,
]

__all__ = [
    "all_sensors",
    # "hubeau_freshness_sensor",  # Disabled
    "error_detection_sensor",
]
