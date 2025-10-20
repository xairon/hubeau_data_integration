"""
Assets de monitoring pour Hub'Eau pipeline
"""

from .data_quality import (
    piezometry_data_quality,
    quality_rivers_data_quality,
    global_data_quality_report,
)

all_monitoring_assets = [
    piezometry_data_quality,
    quality_rivers_data_quality,
    global_data_quality_report,
]

__all__ = [
    "piezometry_data_quality",
    "quality_rivers_data_quality",
    "global_data_quality_report",
    "all_monitoring_assets",
]
