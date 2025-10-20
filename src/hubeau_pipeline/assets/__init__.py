"""
Assets Hub'Eau - Data pipeline layers

Architecture follows Modern Data Stack conventions:
- Raw layer: Hub'Eau APIs → DLT → PostgreSQL (raw data)
- Staging layer: Data cleaning, validation, normalization (future)
- Marts layer: Business-ready metrics and aggregations (future)
- Monitoring layer: Pipeline observability

Standard: dbt/Dagster asset layering pattern
"""

from .raw import all_raw_assets
from .staging import __all__ as staging_assets
from .marts import __all__ as marts_assets
from .monitoring import all_monitoring_assets

# Combine all assets from all layers
all_assets = (
    all_raw_assets +
    staging_assets +
    marts_assets +
    all_monitoring_assets
)

__all__ = [
    "all_assets",
    "all_raw_assets",
    "all_monitoring_assets",
]
