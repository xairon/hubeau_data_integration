"""
TME - Chargement du tableau TME (entités hydrogéologiques, sans géométrie)

Charge TME.csv dans bronze.tme_entites_hydrogeo tel quel. Les jointures avec les
tables ref_*_eh et l'enrichissement des libellés sont faites en silver (stg_tme_entites).

Source : fichier TME.csv à la racine du dépôt.
"""

import re
from pathlib import Path
import pandas as pd
from dagster import AssetExecutionContext, asset
from psycopg2.extras import execute_values

from hubeau_pipeline.resources import PostgreSQLResource


# Chemin par défaut du CSV TME (racine du projet)
DEFAULT_TME_CSV_PATH = "TME.csv"


def _validate_schema_table(schema_name: str, table_name: str) -> None:
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", schema_name):
        raise ValueError(f"schema_name invalide: {schema_name!r}")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
        raise ValueError(f"table_name invalide: {table_name!r}")


# Mapping colonnes CSV -> colonnes bronze (normalisées pour dbt)
CSV_TO_BRONZE = {
    "id": "tme_id",
    "CodeEH": "code_eh",
    "LibelleEH": "libelle_eh",
    "OrdreAbsEH": "ordre_abs_eh",
    "NiveauEH": "niveau_eh",
    "InclusEH": "inclus_eh",
    "EtatEH": "etat_eh",
    "NatureEH": "nature_eh",
    "MilieuEH": "milieu_eh",
    "ThemeEH": "theme_eh",
    "OrigineEH": "origine_eh",
}


@asset(
    description="Table TME (entités hydrogéologiques) depuis TME.csv — intégration normale ; jointures ref_*_eh en silver",
    group_name="bronze",
    compute_kind="python",
)
def tme_entites_hydrogeo(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    """
    Charge TME.csv dans bronze.tme_entites_hydrogeo tel quel (colonnes renommées en snake_case).
    Les jointures avec les tables ref_*_eh et l'enrichissement des libellés sont faites en silver (stg_tme_entites).
    """
    # Résolution du chemin : racine dépôt ou CWD
    repo_root = Path(__file__).resolve().parents[4]  # src/hubeau_pipeline/assets/bronze -> e:\brgm
    path = repo_root / DEFAULT_TME_CSV_PATH
    if not path.exists():
        path = Path.cwd() / DEFAULT_TME_CSV_PATH
    if not path.exists():
        raise FileNotFoundError(f"TME.csv introuvable : {path}")

    context.log.info("Lecture TME.csv : %s", path)
    df = pd.read_csv(path, encoding="utf-8", dtype=str, keep_default_na=False)
    # Renommer colonnes vers schéma bronze (snake_case pour dbt)
    df = df.rename(columns={c: CSV_TO_BRONZE[c] for c in df.columns if c in CSV_TO_BRONZE})
    cols = [c for c in df.columns if c in CSV_TO_BRONZE.values()]
    df = df[cols].copy()

    _validate_schema_table("bronze", "tme_entites_hydrogeo")
    full_table = "bronze.tme_entites_hydrogeo"
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            cur.execute("DROP TABLE IF EXISTS bronze.tme_entites_hydrogeo")
            col_defs = ", ".join(f'"{c}" TEXT' for c in df.columns)
            cur.execute(f"CREATE TABLE bronze.tme_entites_hydrogeo ({col_defs})")
            values = [tuple(row) for row in df.to_numpy().tolist()]
            execute_values(
                cur,
                f'INSERT INTO bronze.tme_entites_hydrogeo ({",".join(chr(34) + c + chr(34) for c in df.columns)}) VALUES %s',
                values,
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_tme_entites_hydrogeo_code_eh ON bronze.tme_entites_hydrogeo (TRIM(code_eh))"
            )
        conn.commit()

    context.log.info("%s alimentée : %s lignes", full_table, len(df))
    return {"table": full_table, "rows": len(df)}
