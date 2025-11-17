"""
Contrôles de qualité de base pour Hub'Eau
"""

from typing import Dict, Any
import pandas as pd
from dagster import AssetExecutionContext, asset, Output, MetadataValue

from hubeau_pipeline.resources import PostgreSQLResource


# ====================================
# CONTRÔLE SIMPLE DE LA BASE
# ====================================

@asset(
    group_name="data_quality",
    description="Vérification basique de la base de données",
)
def basic_database_check(context: AssetExecutionContext, pg: PostgreSQLResource) -> Output[Dict[str, Any]]:
    """
    Vérification basique de la base de données:
    - Tables créées
    - Données présentes
    - Pas d'erreurs évidentes
    """
    report = {}

    with pg.get_connection() as conn:
        # 1. Vérifier les tables existantes
        # ✅ CHANGEMENT #5: PostgreSQL 16 utilise 'relname' au lieu de 'tablename'
        # Utilisation de pg_tables avec LEFT JOIN pour compatibilité PostgreSQL 13+
        df_tables = pd.read_sql("""
            SELECT
                t.schemaname,
                t.tablename,
                pg_size_pretty(pg_total_relation_size(t.schemaname||'.'||t.tablename)) AS size,
                s.n_live_tup AS row_count
            FROM pg_tables t
            LEFT JOIN pg_stat_user_tables s
                ON t.schemaname = s.schemaname
                AND t.tablename = s.relname
            WHERE t.schemaname = 'staging'
            ORDER BY s.n_live_tup DESC NULLS LAST
        """, conn)

        report["tables"] = df_tables.to_dict('records')

        # 2. Vérifier les index géospatiaux
        df_indexes = pd.read_sql("""
            SELECT
                schemaname,
                tablename,
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = 'staging'
            AND indexdef LIKE '%GIST%'
            ORDER BY tablename
        """, conn)

        report["spatial_indexes"] = df_indexes.to_dict('records')

    # 3. Statistiques globales
    total_tables = len(report["tables"])
    total_rows = sum(table["row_count"] for table in report["tables"] if table["row_count"])
    total_spatial_indexes = len(report["spatial_indexes"])

    report["summary"] = {
        "total_tables": total_tables,
        "total_rows": total_rows,
        "total_spatial_indexes": total_spatial_indexes,
        "status": "OK" if total_tables > 0 and total_spatial_indexes > 0 else "PROBLÈME"
    }

    # Métadonnées pour Dagster UI
    metadata = {
        "total_tables": MetadataValue.int(total_tables),
        "total_rows": MetadataValue.int(total_rows),
        "total_spatial_indexes": MetadataValue.int(total_spatial_indexes),
        "status": MetadataValue.text(report["summary"]["status"]),
        "tables": MetadataValue.json(report["tables"]),
    }

    context.log.info(f"✅ Vérification base de données: {report['summary']['status']}")
    context.log.info(f"   Tables: {total_tables}, Lignes: {total_rows:,}, Index géospatiaux: {total_spatial_indexes}")

    return Output(report, metadata=metadata)