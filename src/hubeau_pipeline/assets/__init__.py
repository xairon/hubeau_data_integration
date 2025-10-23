"""
Assets Hub'Eau - Data pipeline layers

Architecture follows Modern Data Stack conventions:
- Bronze layer: Hub'Eau APIs → DLT CSV → PostgreSQL (ingestion directe CSV)
- Raw layer: Hub'Eau APIs → DLT JSON → PostgreSQL (legacy, deprecated)
- Staging layer: Data cleaning, validation, normalization (future)
- Marts layer: Business-ready metrics and aggregations (future)
- Monitoring layer: Pipeline observability

Standard: dbt/Dagster asset layering pattern
"""

from .bronze import all_csv_assets
# DEPRECATED: JSON ingestion layer - commented out for CSV migration
# from .raw import all_raw_assets
from .staging import __all__ as staging_assets
from .marts import __all__ as marts_assets
from .monitoring import all_monitoring_assets

# Combine all assets from all layers
# PRIORITÉ: Bronze (CSV) > Raw (JSON legacy)
all_assets = (
    all_csv_assets +           # NEW: Bronze layer - CSV ingestion
    # all_raw_assets +         # DEPRECATED: Raw layer - JSON ingestion (commented out)
    staging_assets +
    marts_assets +
    all_monitoring_assets
)

__all__ = [
    "all_assets",
    "all_csv_assets",          # NEW: Bronze layer assets
    # "all_raw_assets",        # DEPRECATED: Disabled for CSV migration
    "all_monitoring_assets",
]
