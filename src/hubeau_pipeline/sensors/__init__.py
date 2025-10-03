"""
Sensors - Monitoring et alertes
Surveillance des pipelines et détection d'anomalies
"""

from .data_freshness import hubeau_freshness_sensor
from .error_detection import error_detection_sensor

all_sensors = [
    hubeau_freshness_sensor,
    error_detection_sensor,
]

__all__ = [
    "all_sensors",
    "hubeau_freshness_sensor",
    "error_detection_sensor",
]
