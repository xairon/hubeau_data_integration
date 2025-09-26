"""
Assets Silver - Transformation optimisée vers bases de données cibles
TOUS les assets Hub'Eau avec volumes limités
"""

from .timescale_complete import (
    piezo_timescale_optimized,
    hydro_timescale_optimized,
    temperature_timescale_optimized,
    quality_surface_timescale_optimized,
    quality_groundwater_timescale_optimized
)
from .postgis_neo4j import bdlisa_postgis_silver, sandre_neo4j_silver

__all__ = [
    # TimescaleDB optimisé - TOUS les Hub'Eau
    "piezo_timescale_optimized",
    "hydro_timescale_optimized", 
    "temperature_timescale_optimized",
    "quality_surface_timescale_optimized",
    "quality_groundwater_timescale_optimized",
    
    # PostGIS et Neo4j
    "bdlisa_postgis_silver",
    "sandre_neo4j_silver"
]