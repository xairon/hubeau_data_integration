"""
Assets Hub'Eau - Pipeline d'ingestion simple
Hub'Eau APIs → DLT → PostgreSQL
"""

from .bronze import all_bronze_assets
from .monitoring import all_monitoring_assets

# Tous les assets = ingestion + monitoring
all_assets = all_bronze_assets + all_monitoring_assets

__all__ = ["all_assets"]
