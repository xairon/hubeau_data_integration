"""
Assets de monitoring pour Hub'Eau pipeline
"""

from .data_quality import basic_database_check

all_monitoring_assets = [
    basic_database_check
]

__all__ = [
    "all_monitoring_assets"
]
