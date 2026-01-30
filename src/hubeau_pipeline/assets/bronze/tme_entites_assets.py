"""
TME - Chargement du tableau TME (entités hydrogéologiques, sans géométrie)

Charge le TME depuis l'archive BDLISA (même source que bdlisa_entites_raw) : on télécharge
le zip, on en extrait un CSV entités/TME s'il existe. Sinon on tente l'archive CSV nationale.
Fallback : fichier TME.csv local (dev). Les jointures ref_*_eh sont faites en silver.

Source prioritaire : archive BDLISA (API / téléchargement), config : configs/bdlisa/bdlisa_entites.yml.
"""

import io
import re
from pathlib import Path
from typing import Optional

import httpx
import pandas as pd
from dagster import AssetExecutionContext, asset
from psycopg2.extras import execute_values

from hubeau_pipeline.resources import PostgreSQLResource


# Archive BDLISA CSV nationale (TME / entités en CSV)
BDLISA_NATIONAL_CSV_URL = (
    "https://reseau.eaufrance.fr/geotraitements/bdlisa/files/telechargement/"
    "BDLISA_V3/BDLISA_V3_NATIONAL-csv.zip"
)
DEFAULT_BDLISA_GPKG_URL = (
    "https://reseau.eaufrance.fr/geotraitements/bdlisa/files/telechargement/"
    "BDLISA_V3/BDLISA_V3_METRO-gpkg.zip"
)


def _validate_schema_table(schema_name: str, table_name: str) -> None:
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", schema_name):
        raise ValueError(f"schema_name invalide: {schema_name!r}")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
        raise ValueError(f"table_name invalide: {table_name!r}")


# Mapping colonnes CSV BDLISA -> colonnes bronze (snake_case pour dbt)
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
# Variantes déjà en snake_case (ex. CSV BDLISA normalisé)
for _v in list(CSV_TO_BRONZE.values()):
    if _v not in CSV_TO_BRONZE:
        CSV_TO_BRONZE[_v] = _v

TME_BRONZE_COLUMNS = ["tme_id", "code_eh", "libelle_eh", "ordre_abs_eh", "niveau_eh", "inclus_eh", "etat_eh", "nature_eh", "milieu_eh", "theme_eh", "origine_eh"]


def _load_bdlisa_config(context: AssetExecutionContext) -> dict:
    """Charge la config BDLISA (même que bdlisa_entites_raw)."""
    import yaml
    for base in ("configs", "/app/configs"):
        path = Path(base) / "bdlisa" / "bdlisa_entites.yml"
        if path.exists():
            try:
                with open(path) as f:
                    data = yaml.safe_load(f) or {}
                r = data.get("resource", {})
                perimeters = r.get("perimeters")
                url = r.get("url", DEFAULT_BDLISA_GPKG_URL)
                if perimeters and isinstance(perimeters, list) and perimeters:
                    # On prend la première URL (ex. METRO) pour chercher un CSV dans le zip
                    url = perimeters[0].get("url", url)
                return {"url": url, "perimeters": perimeters}
            except Exception as ex:
                context.log.warning("Config %s ignorée: %s", path, ex)
    return {"url": DEFAULT_BDLISA_GPKG_URL, "perimeters": None}


def _fetch_zip(context: AssetExecutionContext, url: str, timeout: int = 600) -> bytes:
    context.log.info("Téléchargement BDLISA : %s", url)
    with httpx.Client(timeout=timeout) as client:
        resp = client.get(url)
        resp.raise_for_status()
    return resp.content


def _normalize_header(name: str) -> str:
    """Normalise un en-tête CSV pour comparaison (minuscules, espaces/accents -> underscore)."""
    s = str(name).strip().lower()
    s = re.sub(r"[^\w\s]", "", s)  # retire accents etc. (\w = alphanumerique + _)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "col"


def _find_and_read_csv_in_zip(
    zip_bytes: bytes,
    context: Optional[AssetExecutionContext] = None,
) -> Optional[pd.DataFrame]:
    """Cherche un fichier CSV dans le zip BDLISA et retourne un DataFrame (colonnes mappées)."""
    import zipfile
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        names = zf.namelist()
        csv_names = [n for n in names if n.lower().endswith(".csv")]
        if context:
            context.log.info("Fichiers dans le zip : %s", names[:20])
            context.log.info("CSV trouvés : %s", csv_names)
        if not csv_names:
            return None
        csv_names.sort(key=lambda n: (
            0 if ("entite" in n.lower() or "tme" in n.lower() or "entites" in n.lower()) else 1,
            n,
        ))
        first_csv = csv_names[0]
        with zf.open(first_csv) as f:
            df = pd.read_csv(f, encoding="utf-8", dtype=str, keep_default_na=False, on_bad_lines="warn")
        raw_columns = list(df.columns)
        if context:
            context.log.info("CSV lu : %s — colonnes brutes : %s", first_csv, raw_columns)
        # Mapper : d'abord mapping exact, puis normalisé (ex. "Code EH" -> code_eh)
        rename = {}
        normalized_to_bronze = {_normalize_header(c): c for c in TME_BRONZE_COLUMNS}
        for c in df.columns:
            if c in CSV_TO_BRONZE:
                rename[c] = CSV_TO_BRONZE[c]
            elif c.strip() in CSV_TO_BRONZE:
                rename[c] = CSV_TO_BRONZE[c.strip()]
            else:
                n = _normalize_header(c)
                if n in normalized_to_bronze:
                    rename[c] = normalized_to_bronze[n]
        df = df.rename(columns=rename)
        cols = [c for c in TME_BRONZE_COLUMNS if c in df.columns]
        if context and not cols:
            context.log.warning("Aucune colonne TME reconnue dans %s (colonnes après mapping : %s)", first_csv, list(df.columns))
        if not cols:
            return None
        df = df[cols].copy()
        return df


def _read_local_tme_csv(context: AssetExecutionContext) -> Optional[pd.DataFrame]:
    """Fallback : lit TME.csv depuis la racine du dépôt ou CWD."""
    for base in (Path(__file__).resolve().parents[4], Path.cwd()):
        path = base / "TME.csv"
        if path.exists():
            context.log.info("Lecture TME.csv local : %s", path)
            df = pd.read_csv(path, encoding="utf-8", dtype=str, keep_default_na=False)
            df = df.rename(columns={c: CSV_TO_BRONZE[c] for c in df.columns if c in CSV_TO_BRONZE})
            cols = [c for c in df.columns if c in CSV_TO_BRONZE.values()]
            if cols:
                return df[cols].copy()
    return None


@asset(
    description="Table TME (entités hydrogéologiques) depuis l'archive BDLISA ou TME.csv local — jointures ref_*_eh en silver",
    group_name="bronze",
    compute_kind="python",
)
def tme_entites_hydrogeo(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    """
    Charge le TME dans bronze.tme_entites_hydrogeo.
    Source : 1) zip BDLISA (même URL que bdlisa_entites_raw), extraction d'un CSV si présent ;
            2) zip BDLISA CSV national ; 3) fichier TME.csv local. Sinon table vide.
    Fait partie du job reference_data_bronze (chargement des référentiels).
    """
    _validate_schema_table("bronze", "tme_entites_hydrogeo")
    full_table = "bronze.tme_entites_hydrogeo"
    df: Optional[pd.DataFrame] = None
    source = "vide"

    # 1) Zip BDLISA (config = même que gpkg)
    cfg = _load_bdlisa_config(context)
    url = cfg.get("url") or DEFAULT_BDLISA_GPKG_URL
    try:
        zip_bytes = _fetch_zip(context, url)
        df = _find_and_read_csv_in_zip(zip_bytes, context)
        if df is not None and len(df) > 0:
            source = url
    except Exception as e:
        context.log.warning("Zip BDLISA (gpkg) sans CSV ou erreur : %s", e)

    # 2) Si rien, essayer l'archive CSV nationale
    if (df is None or len(df) == 0) and url != BDLISA_NATIONAL_CSV_URL:
        try:
            zip_bytes = _fetch_zip(context, BDLISA_NATIONAL_CSV_URL)
            df = _find_and_read_csv_in_zip(zip_bytes, context)
            if df is not None and len(df) > 0:
                source = BDLISA_NATIONAL_CSV_URL
        except Exception as e:
            context.log.warning("Zip BDLISA CSV national non utilisé : %s", e)

    # 3) Fallback fichier local
    if df is None or len(df) == 0:
        df = _read_local_tme_csv(context)
        if df is not None and len(df) > 0:
            source = "TME.csv (local)"

    if df is None or len(df) == 0:
        context.log.warning("Aucune source TME trouvée — création table vide pour que dbt puisse tourner")
        df = pd.DataFrame(columns=TME_BRONZE_COLUMNS)
    else:
        context.log.info("Source TME : %s — %s lignes", source, len(df))

    # Toujours le même schéma (colonnes attendues par stg_tme_entites)
    for c in TME_BRONZE_COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[TME_BRONZE_COLUMNS]

    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            cur.execute("DROP TABLE IF EXISTS bronze.tme_entites_hydrogeo")
            col_defs = ", ".join(f'"{c}" TEXT' for c in TME_BRONZE_COLUMNS)
            cur.execute(f"CREATE TABLE bronze.tme_entites_hydrogeo ({col_defs})")
            if len(df) > 0:
                values = [tuple(row) for row in df.to_numpy().tolist()]
                execute_values(
                    cur,
                    f'INSERT INTO bronze.tme_entites_hydrogeo ({",".join(chr(34) + c + chr(34) for c in TME_BRONZE_COLUMNS)}) VALUES %s',
                    values,
                )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_tme_entites_hydrogeo_code_eh ON bronze.tme_entites_hydrogeo (TRIM(code_eh))"
            )
        conn.commit()

    context.log.info("%s : %s lignes", full_table, len(df))
    return {"table": full_table, "rows": len(df), "source": source}
