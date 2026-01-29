"""
Sandre Nomenclatures - Chargement des nomenclatures entités hydrogéologiques

Charge les nomenclatures Sandre (PRL / SAQ 2002-1) dans bronze.ref_*_eh.
Source officielle: Sandre (dictionnaire PRL, API definitions).
Données intégrées via pipeline (plus de seeds manuels).
"""

from typing import List, Tuple

from dagster import AssetExecutionContext, asset
from psycopg2.extras import execute_values

from hubeau_pipeline.resources import PostgreSQLResource


# Nomenclatures Sandre / BDLISA (source: Sandre PRL SAQ 2002-1, doc BDLISA)
# https://api.sandre.eaufrance.fr/definitions/v1/dictionnaire/PRL/1.0
REF_NIVEAU_EH: List[Tuple[str, str]] = [
    ("1", "Niveau national"),
    ("2", "Niveau régional"),
    ("3", "Niveau local"),
]
REF_NATURE_EH: List[Tuple[str, str]] = [
    ("0", "Inconnue"),
    ("1", "Grand système aquifère"),
    ("2", "Grand domaine hydrogéologique"),
    ("3", "Système aquifère"),
    ("4", "Domaine hydrogéologique"),
    ("5", "Unité aquifère"),
    ("6", "Unité semi-perméable"),
    ("7", "Unité imperméable"),
    ("12", "Grand système multicouche"),
]
REF_ETAT_EH: List[Tuple[str, str]] = [
    ("0", "Non renseigné"),
    ("1", "Entité hydrogéologique à nappe captive"),
    ("2", "Entité hydrogéologique à nappe libre"),
    ("3", "Entité hydrogéologique à parties libres et captives"),
    ("4", "Entité hydrogéologique alternativement libre puis captive"),
    ("5", "Entité hydrogéologique partiellement captive"),
    ("6", "Non défini (hors nomenclature SAQ 2002-1)"),
]
REF_THEME_EH: List[Tuple[str, str]] = [
    ("0", "Inconnu"),
    ("1", "Alluvial"),
    ("2", "sédimentaire"),
    ("3", "Socle"),
    ("4", "Intensément plissés de montagne"),
    ("5", "Volcanisme"),
]
REF_MILIEU_EH: List[Tuple[str, str]] = [
    ("0", "Inconnu"),
    ("1", "Poreux"),
    ("2", "Fissuré"),
    ("3", "Karstique"),
    ("4", "Poreux et fissuré"),
    ("5", "Fissuré et karstique"),
    ("6", "Multicouche"),
    ("7", "Poreux et karstique"),
    ("8", "Mixte"),
    ("9", "Autre"),
    ("10", "Non applicable"),
]
REF_ORIGINE_EH: List[Tuple[str, str]] = [
    ("1", "Forte potentialité aquifère"),
    ("2", "Potentialité aquifère moyenne"),
    ("3", "Faible potentialité aquifère"),
    ("4", "Nulle ou très faible potentialité"),
]


def _upsert_ref_table(
    conn,
    schema: str,
    table: str,
    rows: List[Tuple[str, str]],
    context: AssetExecutionContext,
) -> int:
    """Crée ou remplace la table ref (code, libelle) et insère les lignes."""
    full = f"{schema}.{table}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f"DROP TABLE IF EXISTS {full}")
        cur.execute(f'CREATE TABLE {full} (code TEXT NOT NULL, libelle TEXT)')
        execute_values(cur, f'INSERT INTO {full} (code, libelle) VALUES %s', rows)
        cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_code ON {full} (code)')
    conn.commit()
    context.log.info(f"  {full}: {len(rows)} lignes")
    return len(rows)


@asset(
    description="Nomenclatures Sandre (entités hydrogéologiques) chargées dans bronze.ref_*_eh — source Sandre PRL / BDLISA",
    group_name="bronze",
    compute_kind="python",
)
def sandre_nomenclatures_eh(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    """
    Charge les nomenclatures Sandre (Niveau, Nature, Etat, Thème, Milieu, Origine)
    dans bronze.ref_niveau_eh, ref_nature_eh, etc.
    Source: Sandre dictionnaire PRL / SAQ 2002-1 (données intégrées via pipeline).
    """
    context.log.info("Chargement nomenclatures Sandre (ref_*_eh)...")
    total = 0
    with pg.get_connection() as conn:
        total += _upsert_ref_table(conn, "bronze", "ref_niveau_eh", REF_NIVEAU_EH, context)
        total += _upsert_ref_table(conn, "bronze", "ref_nature_eh", REF_NATURE_EH, context)
        total += _upsert_ref_table(conn, "bronze", "ref_etat_eh", REF_ETAT_EH, context)
        total += _upsert_ref_table(conn, "bronze", "ref_theme_eh", REF_THEME_EH, context)
        total += _upsert_ref_table(conn, "bronze", "ref_milieu_eh", REF_MILIEU_EH, context)
        total += _upsert_ref_table(conn, "bronze", "ref_origine_eh", REF_ORIGINE_EH, context)
    context.log.info(f"Nomenclatures Sandre chargées: {total} lignes au total")
    return {"tables": 6, "total_rows": total}
