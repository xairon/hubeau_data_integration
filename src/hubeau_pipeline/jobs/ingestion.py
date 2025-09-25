"""
Jobs d'ingestion optimisés - Bronze → Silver
"""

from dagster import define_asset_job, AssetSelection

# Job Hub'Eau : APIs → MinIO → TimescaleDB (optimisé)
hubeau_production_job = define_asset_job(
    name="hubeau_production_job",
    description="🌊 Hub'Eau Production: APIs → MinIO → TimescaleDB (optimisé)",
    selection=AssetSelection.keys(
        # Bronze - Ingestion avec retry/pagination
        "hubeau_piezo_bronze",
        "hubeau_hydro_bronze",
        "hubeau_quality_surface_bronze",
        "hubeau_quality_groundwater_bronze",
        "hubeau_temperature_bronze",

        # Silver - Chargement optimisé TimescaleDB
        "piezo_timescale_optimized",
        "quality_timescale_optimized"
    )
)

# Job BDLISA : WFS → MinIO → PostGIS
bdlisa_production_job = define_asset_job(
    name="bdlisa_production_job",
    description="🗺️ BDLISA Production: WFS → MinIO → PostGIS",
    selection=AssetSelection.keys(
        "bdlisa_geographic_bronze",
        "bdlisa_postgis_silver"
    )
)

# Job Sandre : API → MinIO → Neo4j  
sandre_production_job = define_asset_job(
    name="sandre_production_job",
    description="📚 Sandre Production: API → MinIO → Neo4j",
    selection=AssetSelection.keys(
        "sandre_thesaurus_bronze",
        "sandre_neo4j_silver"
    )
)

# Job Démonstration (simulations pour interface)
demo_showcase_job = define_asset_job(
    name="demo_showcase_job",
    description="🎭 Démonstration: Assets simulés pour interface",
    selection=AssetSelection.keys(
        "demo_quality_scores",
        "demo_neo4j_showcase"
    )
)