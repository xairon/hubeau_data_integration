"""
Assets de validation et qualité des données Hub'Eau
"""

from typing import Dict, Any
import pandas as pd
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import psycopg2
import os


def _get_pg_connection():
    """Crée une connexion PostgreSQL"""
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        database=os.getenv("PG_DB", "postgres"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD"),
    )


# ====================================
# VALIDATION PIÉZOMÉTRIE
# ====================================

@asset(
    group_name="data_quality",
    description="Rapport qualité données piézométrie",
)
def piezometry_data_quality(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Valide la qualité des données piézométrie:
    - Dates cohérentes
    - Coordonnées valides
    - Taux de NULL
    - Outliers
    """
    conn = _get_pg_connection()

    report = {}

    # 1. Stations: Coordonnées valides
    df_coords = pd.read_sql("""
        SELECT
            COUNT(*) AS total_stations,
            COUNT(latitude) AS has_latitude,
            COUNT(longitude) AS has_longitude,
            SUM(CASE WHEN latitude BETWEEN -90 AND 90 THEN 1 ELSE 0 END) AS valid_latitude,
            SUM(CASE WHEN longitude BETWEEN -180 AND 180 THEN 1 ELSE 0 END) AS valid_longitude,
            SUM(CASE WHEN latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180 THEN 1 ELSE 0 END) AS invalid_coords
        FROM hubeau.piezometry_stations
    """, conn)

    report["stations_coordinates"] = df_coords.to_dict('records')[0]

    # 2. Chroniques: Dates cohérentes
    df_dates = pd.read_sql("""
        SELECT
            COUNT(*) AS total_chroniques,
            SUM(CASE WHEN timestamp_mesure > NOW() THEN 1 ELSE 0 END) AS future_dates,
            SUM(CASE WHEN timestamp_mesure < '1900-01-01' THEN 1 ELSE 0 END) AS ancient_dates,
            MIN(timestamp_mesure) AS oldest_measure,
            MAX(timestamp_mesure) AS newest_measure
        FROM hubeau.piezometry_chroniques
    """, conn)

    report["chroniques_dates"] = df_dates.to_dict('records')[0]

    # 3. Chroniques: Taux de NULL sur colonnes critiques
    df_nulls = pd.read_sql("""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN code_bss IS NULL THEN 1 ELSE 0 END) AS null_code_bss,
            SUM(CASE WHEN timestamp_mesure IS NULL THEN 1 ELSE 0 END) AS null_timestamp,
            SUM(CASE WHEN niveau_nappe_ngf IS NULL THEN 1 ELSE 0 END) AS null_niveau_nappe,
            ROUND(100.0 * SUM(CASE WHEN niveau_nappe_ngf IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_niveau
        FROM hubeau.piezometry_chroniques
    """, conn)

    report["chroniques_nulls"] = df_nulls.to_dict('records')[0]

    # 4. Outliers: Niveaux piézométriques anormaux
    df_outliers = pd.read_sql("""
        WITH stats AS (
            SELECT
                AVG(niveau_nappe_ngf) AS mean_niveau,
                STDDEV(niveau_nappe_ngf) AS stddev_niveau
            FROM hubeau.piezometry_chroniques
            WHERE niveau_nappe_ngf IS NOT NULL
        )
        SELECT
            COUNT(*) AS total_measures,
            SUM(CASE WHEN niveau_nappe_ngf < (mean_niveau - 3 * stddev_niveau) THEN 1 ELSE 0 END) AS outliers_low,
            SUM(CASE WHEN niveau_nappe_ngf > (mean_niveau + 3 * stddev_niveau) THEN 1 ELSE 0 END) AS outliers_high,
            mean_niveau,
            stddev_niveau
        FROM hubeau.piezometry_chroniques, stats
        WHERE niveau_nappe_ngf IS NOT NULL
    """, conn)

    report["chroniques_outliers"] = df_outliers.to_dict('records')[0]

    # 5. Données orphelines (si foreign keys pas encore créées)
    df_orphans = pd.read_sql("""
        SELECT COUNT(*) AS orphaned_chroniques
        FROM hubeau.piezometry_chroniques pc
        LEFT JOIN hubeau.piezometry_stations ps ON pc.code_bss = ps.code_station
        WHERE ps.code_station IS NULL
    """, conn)

    report["orphaned_data"] = df_orphans.to_dict('records')[0]

    conn.close()

    # Calcul score qualité (0-100)
    total_stations = report["stations_coordinates"]["total_stations"]
    invalid_coords = report["stations_coordinates"]["invalid_coords"]
    future_dates = report["chroniques_dates"]["future_dates"]
    orphans = report["orphaned_data"]["orphaned_chroniques"]

    penalties = 0
    if total_stations > 0:
        penalties += (invalid_coords / total_stations) * 20
    penalties += min(future_dates, 100) * 0.1
    penalties += min(orphans, 1000) * 0.01

    quality_score = max(0, 100 - penalties)
    report["quality_score"] = round(quality_score, 2)

    # Metadata pour Dagster UI
    metadata = {
        "quality_score": MetadataValue.float(quality_score),
        "total_stations": MetadataValue.int(total_stations),
        "invalid_coords": MetadataValue.int(invalid_coords),
        "future_dates": MetadataValue.int(future_dates),
        "orphaned_chroniques": MetadataValue.int(orphans),
        "report": MetadataValue.json(report),
    }

    context.log.info(f"✅ Qualité piézométrie: {quality_score}/100")
    context.log.info(f"   Stations: {total_stations}, coordonnées invalides: {invalid_coords}")
    context.log.info(f"   Dates futures: {future_dates}, orphelins: {orphans}")

    return Output(report, metadata=metadata)


# ====================================
# VALIDATION QUALITÉ COURS D'EAU
# ====================================

@asset(
    group_name="data_quality",
    description="Rapport qualité données qualité cours d'eau",
)
def quality_rivers_data_quality(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Valide la qualité des données qualité cours d'eau:
    - Paramètres valides
    - Résultats cohérents
    - Taux de NULL
    """
    conn = _get_pg_connection()

    report = {}

    # 1. Distribution des paramètres
    df_params = pd.read_sql("""
        SELECT
            code_parametre,
            libelle_parametre,
            COUNT(*) AS nb_analyses,
            COUNT(DISTINCT code_station) AS nb_stations,
            AVG(resultat::float) AS moyenne,
            MIN(resultat::float) AS minimum,
            MAX(resultat::float) AS maximum
        FROM hubeau.quality_rivers_analyses
        WHERE code_parametre IS NOT NULL
        GROUP BY code_parametre, libelle_parametre
        ORDER BY nb_analyses DESC
        LIMIT 20
    """, conn)

    report["top_parametres"] = df_params.to_dict('records')

    # 2. Taux de NULL sur résultats
    df_nulls = pd.read_sql("""
        SELECT
            COUNT(*) AS total_analyses,
            SUM(CASE WHEN resultat IS NULL THEN 1 ELSE 0 END) AS null_resultat,
            ROUND(100.0 * SUM(CASE WHEN resultat IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_resultat
        FROM hubeau.quality_rivers_analyses
    """, conn)

    report["nulls"] = df_nulls.to_dict('records')[0]

    # 3. Dates cohérentes
    df_dates = pd.read_sql("""
        SELECT
            COUNT(*) AS total_analyses,
            SUM(CASE WHEN date_prelevement > NOW() THEN 1 ELSE 0 END) AS future_dates,
            MIN(date_prelevement) AS oldest_sample,
            MAX(date_prelevement) AS newest_sample
        FROM hubeau.quality_rivers_analyses
    """, conn)

    report["dates"] = df_dates.to_dict('records')[0]

    conn.close()

    # Score qualité
    total_analyses = report["nulls"]["total_analyses"]
    null_resultat = report["nulls"]["null_resultat"]
    future_dates = report["dates"]["future_dates"]

    penalties = 0
    if total_analyses > 0:
        penalties += (null_resultat / total_analyses) * 30
    penalties += min(future_dates, 100) * 0.2

    quality_score = max(0, 100 - penalties)
    report["quality_score"] = round(quality_score, 2)

    metadata = {
        "quality_score": MetadataValue.float(quality_score),
        "total_analyses": MetadataValue.int(total_analyses),
        "null_resultat_pct": MetadataValue.float(report["nulls"]["pct_null_resultat"]),
        "nb_parametres": MetadataValue.int(len(report["top_parametres"])),
    }

    context.log.info(f"✅ Qualité cours d'eau: {quality_score}/100")
    context.log.info(f"   Analyses: {total_analyses}, NULL résultats: {report['nulls']['pct_null_resultat']}%")

    return Output(report, metadata=metadata)


# ====================================
# RAPPORT GLOBAL
# ====================================

@asset(
    group_name="data_quality",
    description="Rapport qualité global toutes APIs",
    deps=[piezometry_data_quality, quality_rivers_data_quality],
)
def global_data_quality_report(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Agrège tous les rapports de qualité en un rapport global
    """
    conn = _get_pg_connection()

    report = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "tables": {},
    }

    # Statistiques par table
    tables_query = """
        SELECT
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
            n_live_tup AS row_count,
            n_dead_tup AS dead_rows,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables
        WHERE schemaname = 'hubeau'
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
    """

    df_tables = pd.read_sql(tables_query, conn)
    report["tables"] = df_tables.to_dict('records')

    # Statistiques DLT
    dlt_query = """
        SELECT
            pipeline_name,
            COUNT(*) AS nb_loads,
            MAX(inserted_at) AS last_load,
            SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS successful_loads,
            SUM(CASE WHEN status != 0 THEN 1 ELSE 0 END) AS failed_loads
        FROM hubeau._dlt_loads
        GROUP BY pipeline_name
        ORDER BY last_load DESC
    """

    df_dlt = pd.read_sql(dlt_query, conn)
    report["dlt_pipelines"] = df_dlt.to_dict('records')

    conn.close()

    # Calculer nombre total de lignes
    total_rows = sum(t['row_count'] for t in report['tables'] if t['row_count'])
    total_size = df_tables.iloc[0]['size'] if len(df_tables) > 0 else "0 bytes"

    metadata = {
        "total_tables": MetadataValue.int(len(report["tables"])),
        "total_rows": MetadataValue.int(total_rows),
        "total_size": MetadataValue.text(str(total_size)),
        "nb_pipelines": MetadataValue.int(len(report["dlt_pipelines"])),
    }

    context.log.info(f"✅ Rapport global:")
    context.log.info(f"   Tables: {len(report['tables'])}")
    context.log.info(f"   Lignes totales: {total_rows:,}")
    context.log.info(f"   Pipelines DLT: {len(report['dlt_pipelines'])}")

    return Output(report, metadata=metadata)
