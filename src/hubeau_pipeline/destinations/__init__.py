"""Custom DLT destinations optimisées pour PostgreSQL"""

from .postgres_optimized_v2 import postgres_bulk_destination_v2 as postgres_bulk_destination, get_postgres_destination

__all__ = ["postgres_bulk_destination", "get_postgres_destination"]
