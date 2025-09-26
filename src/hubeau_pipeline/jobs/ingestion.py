"""
Jobs d'ingestion optimisés - Bronze → Silver (Hub'Eau 8 APIs)
"""

from dagster import define_asset_job, AssetSelection

# Job Hub'Eau Principal : 5 APIs quotidiennes → TimescaleDB
hubeau_production_job = define_asset_job(
    name="hubeau_production_job",
    description="🌊 Hub'Eau Principal: 5 APIs quotidiennes → TimescaleDB",
    selection=AssetSelection.keys(
        # Bronze - APIs principales (quotidiennes)
        "hubeau_piezo_bronze",
        "hubeau_hydro_bronze",
        "hubeau_quality_surface_bronze",
        "hubeau_quality_groundwater_bronze",
        "hubeau_temperature_bronze",

        # Silver - Chargement optimisé TimescaleDB (5 APIs)
        "piezo_timescale_optimized",
        "hydro_timescale_optimized",
        "quality_surface_timescale_optimized",
        "quality_groundwater_timescale_optimized",
        "temperature_timescale_optimized"
    )
)

# Job Hub'Eau Complémentaire : 3 APIs mensuelles/saisonnières
hubeau_complementary_job = define_asset_job(
    name="hubeau_complementary_job", 
    description="🌊 Hub'Eau Complémentaire: Écoulement + Hydrobiologie + Prélèvements",
    selection=AssetSelection.keys(
        # Bronze - APIs complémentaires (mensuelles)
        "hubeau_ecoulement_bronze",
        "hubeau_hydrobiologie_bronze",
        "hubeau_prelevements_bronze",

        # Silver - Chargement complémentaire TimescaleDB
        "ecoulement_timescale",
        "hydrobiologie_timescale", 
        "prelevements_timescale"
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