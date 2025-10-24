"""
Sensors - Monitoring et alertes
"""

from .error_detection import error_detection_sensor
from .failure_sensor import (
    pipeline_failure_alert_sensor,
    long_running_pipeline_sensor,
    repeated_failure_sensor,
)

all_sensors = [
    error_detection_sensor,
    pipeline_failure_alert_sensor,
    long_running_pipeline_sensor,
    repeated_failure_sensor,
]

__all__ = [
    "all_sensors",
    "error_detection_sensor",
    "pipeline_failure_alert_sensor",
    "long_running_pipeline_sensor",
    "repeated_failure_sensor",
]
