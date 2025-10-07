"""
Assets de monitoring pour Hub'Eau pipeline
"""
from .minio_monitoring import (
    minio_data_quality_report,
    minio_data_quality_checks
)

__all__ = [
    "minio_data_quality_report",
    "minio_data_quality_checks"
]
