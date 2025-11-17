"""
Dagster Sensors - Hub'Eau Pipeline

Sensors auto-detect events and trigger runs:
- csv_file_watcher: Auto-ingest new CSV files from inbox
- csv_archive_cleaner: Archive processed CSV files
"""

from .csv_sensor import (
    csv_file_watcher_sensor,
    csv_archive_cleaner_sensor
)

all_sensors = [
    csv_file_watcher_sensor,
    csv_archive_cleaner_sensor,
]

__all__ = [
    "all_sensors",
    "csv_file_watcher_sensor",
    "csv_archive_cleaner_sensor",
]
