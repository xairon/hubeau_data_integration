"""
Sandre Nomenclatures - Chargement des nomenclatures TME BD LISA

Charge les nomenclatures Sandre (dictionnaire Hydrogéologie SAQ / BD LISA) dans
bronze.ref_*_eh pour donner les libellés des colonnes NatureEH, MilieuEH, ThemeEH,
EtatEH, OrigineEH du Tableau Multi-Échelles (TME).

Correspondances officielles : NatureEH→n°339, MilieuEH→338, ThemeEH→348,
EtatEH→349, OrigineEH→341. NiveauEH (1/2/3) en fallback (pas de nomenclature NSA).

Source: API Sandre https://api.sandre.eaufrance.fr/referentiels/v1/nsa.json
"""

import re
from typing import List, Tuple

import httpx
from dagster import AssetExecutionContext, asset
from psycopg2.extras import execute_values

from hubeau_pipeline.resources import PostgreSQLResource


# API Sandre - Référentiel NSA (nomenclatures)
# Doc: dictionnaire Hydrogéologie SAQ / BD LISA — TME (Tableau Multi-Échelles)
SANDRE_NSA_URL = "https://api.sandre.eaufrance.fr/referentiels/v1/nsa.json"

# Mapping numéro Sandre (CdReferentiel) -> table bronze
# Correspondances officielles colonnes TME BD LISA / nomenclatures Sandre :
# NatureEH→339, MilieuEH→338, ThemeEH→348, EtatEH→349, OrigineEH→341
SANDRE_REF_TO_TABLE: dict[str, str] = {
    "339": "ref_nature_eh",   # Nature de l'entité hydrogéologique
    "338": "ref_milieu_eh",   # Milieu de l'entité hydrogéologique
    "348": "ref_theme_eh",    # Thème de l'entité hydrogéologique
    "349": "ref_etat_eh",    # État de l'entité hydrogéologique
    "341": "ref_origine_eh", # Origine de l'entité hydrogéologique
}

# Fallback : NiveauEH (hiérarchie BD LISA 1=National, 2=Régional, 3=Local) — pas de nomenclature NSA dédiée
REF_NIVEAU_EH: List[Tuple[str, str]] = [
    ("1", "Niveau national"),
    ("2", "Niveau régional"),
    ("3", "Niveau local"),
]


def _referentiels_from_nsa(data: dict) -> List[dict]:
    """Extrait la liste des référentiels du JSON NSA (Referentiel peut être objet ou liste)."""
    refs = (data.get("REFERENTIELS") or {}).get("Referentiel")
    if refs is None:
        return []
    return refs if isinstance(refs, list) else [refs]


def _elements_from_referentiel(ref: dict) -> List[Tuple[str, str]]:
    """Extrait (code, libelle) des éléments d'un référentiel Sandre."""
    elements = ref.get("Element")
    if not elements:
        return []
    if isinstance(elements, dict):
        elements = [elements]
    rows: List[Tuple[str, str]] = []
    for el in elements:
        code = el.get("CdElement") or el.get("cdElement", "")
        libelle = el.get("LbElement") or el.get("lbElement", "") or ""
        rows.append((str(code).strip(), str(libelle).strip()))
    return rows


def _fetch_sandre_nsa(context: AssetExecutionContext) -> dict:
    """Télécharge le référentiel NSA depuis l'API Sandre."""
    context.log.info("Téléchargement API Sandre NSA: %s", SANDRE_NSA_URL)
    with httpx.Client(timeout=120.0) as client:
        resp = client.get(SANDRE_NSA_URL)
        resp.raise_for_status()
    return resp.json()


def _validate_schema_table(schema_name: str, table_name: str) -> None:
    """Valide schéma et table pour éviter injection SQL (alphanumerique + underscore)."""
    if not (schema_name and table_name):
        raise ValueError("schema_name et table_name requis")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", schema_name):
        raise ValueError(f"schema_name invalide: {schema_name!r}")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
        raise ValueError(f"table_name invalide: {table_name!r}")


def _upsert_ref_table(
    conn,
    schema: str,
    table: str,
    rows: List[Tuple[str, str]],
    context: AssetExecutionContext,
) -> int:
    """Crée ou remplace la table ref (code, libelle) et insère les lignes."""
    _validate_schema_table(schema, table)
    full = f"{schema}.{table}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f"DROP TABLE IF EXISTS {full}")
        cur.execute(f"CREATE TABLE {full} (code TEXT NOT NULL, libelle TEXT)")
        execute_values(cur, f"INSERT INTO {full} (code, libelle) VALUES %s", rows)
        cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_code ON {full} (code)")
    conn.commit()
    context.log.info("  %s: %s lignes", full, len(rows))
    return len(rows)


@asset(
    description="Nomenclatures Sandre (entités hydrogéologiques) chargées dans bronze.ref_*_eh — source API Sandre NSA + fallback BDLISA",
    group_name="bronze",
    compute_kind="python",
)
def sandre_nomenclatures_eh(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    """
    Charge les nomenclatures Sandre (Niveau, Nature, Etat, Thème, Milieu, Origine)
    dans bronze.ref_*_eh pour le TME (Tableau Multi-Échelles) BD LISA.
    Source: API nsa.json — n°339 (Nature), 338 (Milieu), 348 (Thème), 349 (État), 341 (Origine).
    Niveau (1/2/3) en fallback.
    """
    context.log.info("Chargement nomenclatures Sandre (ref_*_eh)...")
    total = 0

    # 1) Chargement depuis l'API Sandre pour les nomenclatures NSA
    try:
        data = _fetch_sandre_nsa(context)
        refs = _referentiels_from_nsa(data)
        with pg.get_connection() as conn:
            for ref in refs:
                cd = ref.get("CdReferentiel") or ref.get("cdReferentiel")
                if cd is None:
                    continue
                cd_str = str(cd).strip()
                table = SANDRE_REF_TO_TABLE.get(cd_str)
                if not table:
                    continue
                rows = _elements_from_referentiel(ref)
                if rows:
                    total += _upsert_ref_table(conn, "bronze", table, rows, context)
    except Exception as e:
        context.log.warning("API Sandre NSA indisponible ou erreur: %s — utilisation des fallbacks uniquement.", e)
        # En cas d'erreur, on continue avec les fallbacks pour niveau et origine
        # mais les 4 autres tables ne seront pas remplies par l'API → on les remplit en fallback ci‑dessous
        data = None
        refs = []

    # 2) NiveauEH (1=National, 2=Régional, 3=Local) — toujours en fallback (pas de nomenclature NSA)
    with pg.get_connection() as conn:
        total += _upsert_ref_table(conn, "bronze", "ref_niveau_eh", REF_NIVEAU_EH, context)

    # 3) Si l'API n'a rien renvoyé (ou erreur), fallback en dur pour les 5 nomenclatures TME
    if data is None or not refs:
        from hubeau_pipeline.assets.bronze._sandre_fallback import (
            REF_ETAT_EH,
            REF_MILIEU_EH,
            REF_NATURE_EH,
            REF_ORIGINE_EH,
            REF_THEME_EH,
        )

        with pg.get_connection() as conn:
            total += _upsert_ref_table(conn, "bronze", "ref_nature_eh", REF_NATURE_EH, context)
            total += _upsert_ref_table(conn, "bronze", "ref_etat_eh", REF_ETAT_EH, context)
            total += _upsert_ref_table(conn, "bronze", "ref_theme_eh", REF_THEME_EH, context)
            total += _upsert_ref_table(conn, "bronze", "ref_milieu_eh", REF_MILIEU_EH, context)
            total += _upsert_ref_table(conn, "bronze", "ref_origine_eh", REF_ORIGINE_EH, context)

    context.log.info("Nomenclatures Sandre chargées: %s lignes au total", total)
    return {"tables": 6, "total_rows": total}
