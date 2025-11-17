"""
Assets Dagster pour charger les entités hydrogéologiques BD-LISA
"""

import os
from dagster import asset, AssetExecutionContext, multi_asset, AssetOut
from typing import Dict, Any, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor

from hubeau_pipeline.sources.bdlisa_raw_source import bdlisa_raw
from hubeau_pipeline.utils.dlt_batching import (
    create_dlt_pipeline,
    run_dlt_resource,
)
def _setup_postgis_geometry(table_name: str, context: AssetExecutionContext):
    """Configure la colonne geometry PostGIS après le chargement"""

    context.log.info(f"Setting up PostGIS geometry for {table_name}...")

    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST", "localhost"),
            database=os.getenv("PG_DB", "postgres"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD"),
            port=int(os.getenv("PG_PORT", "5432"))
        )

        with conn.cursor() as cur:
            # Vérifier que PostGIS est installé
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

            # Ajouter une colonne geometry typée si elle n'existe pas
            cur.execute(f"""
                ALTER TABLE bdlisa.{table_name}
                ADD COLUMN IF NOT EXISTS geom geometry(Geometry, 2154);
            """)

            # Convertir le WKT en geometry PostGIS
            cur.execute(f"""
                UPDATE bdlisa.{table_name}
                SET geom = ST_GeomFromText(geometry_wkt, 2154)
                WHERE geometry_wkt IS NOT NULL
                AND geometry_wkt != ''
                AND geometry_wkt NOT LIKE '{{%';
            """)

            # Si c'est du JSON (fallback), essayer de le convertir
            cur.execute(f"""
                UPDATE bdlisa.{table_name}
                SET geom = ST_GeomFromGeoJSON(geometry_wkt)
                WHERE geometry_wkt IS NOT NULL
                AND geometry_wkt LIKE '{{%'
                AND geom IS NULL;
            """)

            # Créer un index spatial pour performances
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_geom
                ON bdlisa.{table_name} USING GIST(geom);
            """)

            # Créer des index sur les colonnes principales
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_code
                ON bdlisa.{table_name}(code) WHERE code IS NOT NULL;

                CREATE INDEX IF NOT EXISTS idx_{table_name}_niveau
                ON bdlisa.{table_name}(niveau) WHERE niveau IS NOT NULL;

                CREATE INDEX IF NOT EXISTS idx_{table_name}_theme
                ON bdlisa.{table_name}(theme) WHERE theme IS NOT NULL;

                CREATE INDEX IF NOT EXISTS idx_{table_name}_nature
                ON bdlisa.{table_name}(nature) WHERE nature IS NOT NULL;
            """)

            conn.commit()
            context.log.info(f"PostGIS geometry setup completed for {table_name}")

    except Exception as e:
        context.log.warning(f"Could not setup PostGIS geometry for {table_name}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


@multi_asset(
    outs={
        "bdlisa_entites_nv1": AssetOut(description="Entités hydrogéologiques niveau 1 (national)"),
        "bdlisa_entites_nv2": AssetOut(description="Entités hydrogéologiques niveau 2 (régional)"),
        "bdlisa_entites_nv3": AssetOut(description="Entités hydrogéologiques niveau 3 (local)"),
    },
    group_name="bdlisa_spatial",
    description="Entités hydrogéologiques BD-LISA avec géométries"
)
def bdlisa_entites(context: AssetExecutionContext) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Charge toutes les entités BD-LISA avec leurs géométries"""

    context.log.info("Loading BD-LISA hydrogeological entities...")

    pipeline = create_dlt_pipeline(
        "bdlisa",
        context=context,
        dataset_name="bdlisa",
    )
    source = bdlisa_raw()

    results = {}

    # Charger niveau 1
    context.log.info("Loading BD-LISA niveau 1...")
    metrics_nv1 = run_dlt_resource(
        pipeline=pipeline,
        resource=source.entites_niveau1,
        context=context,
        table_name="bdlisa_entites_nv1",
        write_disposition="replace",
    )

    _setup_postgis_geometry("bdlisa_entites_nv1", context)
    results["bdlisa_entites_nv1"] = metrics_nv1
    context.log.info(
        "Loaded %s entities niveau 1",
        metrics_nv1.get("rows_loaded", 0),
    )

    # Charger niveau 2
    context.log.info("Loading BD-LISA niveau 2...")
    metrics_nv2 = run_dlt_resource(
        pipeline=pipeline,
        resource=source.entites_niveau2,
        context=context,
        table_name="bdlisa_entites_nv2",
        write_disposition="replace",
    )

    _setup_postgis_geometry("bdlisa_entites_nv2", context)
    results["bdlisa_entites_nv2"] = metrics_nv2
    context.log.info(
        "Loaded %s entities niveau 2",
        metrics_nv2.get("rows_loaded", 0),
    )

    # Charger niveau 3
    context.log.info("Loading BD-LISA niveau 3...")
    metrics_nv3 = run_dlt_resource(
        pipeline=pipeline,
        resource=source.entites_niveau3,
        context=context,
        table_name="bdlisa_entites_nv3",
        write_disposition="replace",
    )

    _setup_postgis_geometry("bdlisa_entites_nv3", context)
    results["bdlisa_entites_nv3"] = metrics_nv3
    context.log.info(
        "Loaded %s entities niveau 3",
        metrics_nv3.get("rows_loaded", 0),
    )

    # Log summary
    total_rows = (
        metrics_nv1.get("rows_loaded", 0)
        + metrics_nv2.get("rows_loaded", 0)
        + metrics_nv3.get("rows_loaded", 0)
    )
    context.log.info(f"BD-LISA loading complete: {total_rows} total entities across 3 levels")

    return (
        results.get("bdlisa_entites_nv1", {}),
        results.get("bdlisa_entites_nv2", {}),
        results.get("bdlisa_entites_nv3", {})
    )


@asset(
    group_name="bdlisa_spatial",
    description="Statistiques des entités BD-LISA chargées",
    deps=["bdlisa_entites_nv1", "bdlisa_entites_nv2", "bdlisa_entites_nv3"]
)
def bdlisa_stats(context: AssetExecutionContext) -> Dict[str, Any]:
    """Calcule des statistiques sur les entités BD-LISA chargées"""

    stats = {}

    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST", "localhost"),
            database=os.getenv("PG_DB", "postgres"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD"),
            port=int(os.getenv("PG_PORT", "5432")),
            cursor_factory=RealDictCursor
        )

        with conn.cursor() as cur:
            # Stats niveau 1
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(DISTINCT theme) as themes,
                    COUNT(DISTINCT nature) as natures,
                    COUNT(geom) as with_geometry
                FROM bdlisa.bdlisa_entites_nv1
            """)
            stats['niveau1'] = dict(cur.fetchone())

            # Stats niveau 2
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(DISTINCT theme) as themes,
                    COUNT(DISTINCT nature) as natures,
                    COUNT(geom) as with_geometry
                FROM bdlisa.bdlisa_entites_nv2
            """)
            stats['niveau2'] = dict(cur.fetchone())

            # Stats niveau 3
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(DISTINCT theme) as themes,
                    COUNT(DISTINCT nature) as natures,
                    COUNT(geom) as with_geometry
                FROM bdlisa.bdlisa_entites_nv3
            """)
            stats['niveau3'] = dict(cur.fetchone())

            # Surface totale couverte (km²)
            cur.execute("""
                SELECT
                    ROUND(SUM(ST_Area(geom)) / 1000000) as surface_km2
                FROM bdlisa.bdlisa_entites_nv1
                WHERE geom IS NOT NULL
            """)
            result = cur.fetchone()
            stats['surface_totale_km2'] = result['surface_km2'] if result else 0

        conn.close()

        context.log.info(f"BD-LISA statistics: {stats}")

    except Exception as e:
        context.log.warning(f"Could not compute BD-LISA stats: {e}")
        stats = {"error": str(e)}

    return stats