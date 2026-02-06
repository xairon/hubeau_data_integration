"""
TME - Chargement du tableau TME (entités hydrogéologiques, sans géométrie)

Charge le TME depuis l'archive BDLISA (même source que bdlisa_entites_raw) : on télécharge
le zip, on en extrait un CSV entités/TME s'il existe. Sinon on tente l'archive CSV nationale.
Fallback : fichier TME.csv local (dev). Les jointures ref_*_eh sont faites en silver.

Source prioritaire : archive BDLISA (API / téléchargement), config : configs/bdlisa/bdlisa_entites.yml.
"""

import io
import os
import re
from pathlib import Path
from typing import Optional

import httpx
import pandas as pd
from dagster import AssetExecutionContext, asset
from psycopg2 import sql
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
    "Code EH": "code_eh",
    "Code entite": "code_eh",
    "Code entité": "code_eh",
    "Code_entite": "code_eh",
    "LibelleEH": "libelle_eh",
    "Libelle EH": "libelle_eh",
    "Libelle entite": "libelle_eh",
    "Libelle entité": "libelle_eh",
    "OrdreAbsEH": "ordre_abs_eh",
    "OrdreAbs EH": "ordre_abs_eh",
    "NiveauEH": "niveau_eh",
    "Niveau EH": "niveau_eh",
    "InclusEH": "inclus_eh",
    "Inclus EH": "inclus_eh",
    "EtatEH": "etat_eh",
    "Etat EH": "etat_eh",
    "NatureEH": "nature_eh",
    "Nature EH": "nature_eh",
    "MilieuEH": "milieu_eh",
    "Milieu EH": "milieu_eh",
    "ThemeEH": "theme_eh",
    "Theme EH": "theme_eh",
    "OrigineEH": "origine_eh",
    "Origine EH": "origine_eh",
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


def _read_one_csv_from_zip(
    zf: "zipfile.ZipFile",
    csv_path: str,
    context: Optional[AssetExecutionContext] = None,
) -> Optional[pd.DataFrame]:
    """Lit un CSV du zip, mappe les colonnes vers TME_BRONZE_COLUMNS, retourne un DataFrame ou None."""
    try:
        with zf.open(csv_path) as f:
            raw = f.read()
    except Exception as e:
        if context:
            context.log.warning("Impossible de lire %s : %s", csv_path, e)
        return None

    def _read_with_encoding(sep: str):
        try:
            return pd.read_csv(
                io.BytesIO(raw), encoding="utf-8", dtype=str, keep_default_na=False,
                on_bad_lines="warn", sep=sep
            )
        except UnicodeDecodeError:
            return pd.read_csv(
                io.BytesIO(raw), encoding="latin-1", dtype=str, keep_default_na=False,
                on_bad_lines="warn", sep=sep
            )

    df = _read_with_encoding(";")
    if df.shape[1] <= 1 and len(df.columns) == 1 and ("," in str(df.columns[0]) or ";" in str(df.columns[0])):
        df = _read_with_encoding(",")
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
    if not cols:
        if context:
            context.log.warning(
                "Aucune colonne TME reconnue dans %s (colonnes: %s)",
                csv_path,
                list(df.columns),
            )
        return None
    if "code_eh" not in df.columns:
        if context:
            context.log.warning(
                "Colonne code_eh absente dans %s (colonnes: %s)",
                csv_path,
                list(df.columns),
            )
        return None
    # Exiger au moins un attribut TME (évite de charger des tables de codes département)
    attr_candidates = [
        "libelle_eh",
        "niveau_eh",
        "etat_eh",
        "nature_eh",
        "milieu_eh",
        "theme_eh",
        "origine_eh",
        "ordre_abs_eh",
        "inclus_eh",
    ]
    if not any(c in df.columns for c in attr_candidates):
        if context:
            context.log.info(
                "CSV ignoré (pas d'attributs TME) : %s (colonnes: %s)",
                csv_path,
                list(df.columns),
            )
        return None
    for c in TME_BRONZE_COLUMNS:
        if c not in df.columns:
            df[c] = None
    return df[TME_BRONZE_COLUMNS].copy()


def _read_all_tme_csv_from_zip(
    zip_bytes: bytes,
    context: Optional[AssetExecutionContext] = None,
) -> Optional[pd.DataFrame]:
    """Lit tous les CSV TME/entités du zip, les concatène et dédoublonne par code_eh (tous périmètres)."""
    import zipfile
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        names = zf.namelist()
        csv_names = [n for n in names if n.lower().endswith(".csv")]
        tme_like = [n for n in csv_names if "tme" in n.lower() or "entite" in n.lower() or "entites" in n.lower()]
        to_read = sorted(tme_like) if tme_like else sorted(csv_names)
        if context:
            context.log.info("CSV à fusionner (%s) : %s", len(to_read), to_read[:15])
        dfs = []
        for csv_path in to_read:
            df = _read_one_csv_from_zip(zf, csv_path, context)
            if df is not None and len(df) > 0 and "code_eh" in df.columns:
                dfs.append(df)
                if context:
                    context.log.info("Lu %s : %s lignes", csv_path, len(df))
        if not dfs:
            return None
        out = pd.concat(dfs, ignore_index=True)
        out["code_eh"] = out["code_eh"].astype(str).str.strip()
        # Exclure les codes département / agrégat (ex. 104, 121, 974) : garder uniquement
        # les codes entité BDLISA (ex. 050AA01, 974AH) qui contiennent au moins une lettre.
        before = len(out)
        out = out[out["code_eh"].str.contains(r"[A-Za-z]", na=False)]
        if context and len(out) < before:
            context.log.info("TME : %s lignes exclues (codes département sans lettre)", before - len(out))

        # Garder la meilleure ligne par code_eh : on privilégie celles avec attributs renseignés.
        attr_cols = [
            "libelle_eh",
            "ordre_abs_eh",
            "niveau_eh",
            "inclus_eh",
            "etat_eh",
            "nature_eh",
            "milieu_eh",
            "theme_eh",
            "origine_eh",
        ]
        for c in attr_cols:
            if c in out.columns:
                out[c] = out[c].replace("", None)
        out["_attr_score"] = out[attr_cols].notna().sum(axis=1)
        out = out.sort_values(["code_eh", "_attr_score"], ascending=[True, False])
        out = out.drop_duplicates(subset=["code_eh"], keep="first").drop(columns=["_attr_score"])

        if context:
            context.log.info("TME fusionné : %s lignes (codes entité uniquement)", len(out))
        return out if len(out) > 0 else None


def _find_and_read_csv_in_zip(
    zip_bytes: bytes,
    context: Optional[AssetExecutionContext] = None,
    concat_all: bool = False,
) -> Optional[pd.DataFrame]:
    """Cherche un ou plusieurs CSV dans le zip BDLISA et retourne un DataFrame (colonnes mappées).
    Si concat_all=True, lit et fusionne tous les CSV TME/entités (pour zip national multi-périmètres)."""
    if concat_all:
        return _read_all_tme_csv_from_zip(zip_bytes, context)
    import zipfile
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        names = zf.namelist()
        csv_names = [n for n in names if n.lower().endswith(".csv")]
        if context:
            context.log.info("Fichiers dans le zip : %s", names[:20])
            context.log.info("CSV trouvés : %s", csv_names)
        if not csv_names:
            return None
        tme_csv = [n for n in csv_names if n.endswith("TME.csv") or n.endswith("/TME.csv")]
        if tme_csv:
            first_csv = tme_csv[0]
        else:
            csv_names.sort(key=lambda n: (
                0 if ("entite" in n.lower() or "tme" in n.lower() or "entites" in n.lower()) else 1,
                n,
            ))
            first_csv = csv_names[0]
        return _read_one_csv_from_zip(zf, first_csv, context)


def _read_local_tme_csv(context: AssetExecutionContext) -> Optional[pd.DataFrame]:
    """Fallback : lit TME.csv depuis la racine du dépôt ou CWD."""
    extra_paths = []
    env_dir = os.environ.get("BDLISA_CSV_DIR")
    if env_dir:
        extra_paths.append(Path(env_dir) / "TME.csv")
    for base in (Path(__file__).resolve().parents[4], Path.cwd()):
        extra_paths.append(base / "TME.csv")

    for path in extra_paths:
        if path.exists():
            context.log.info("Lecture TME.csv local : %s", path)
            df = pd.read_csv(path, encoding="utf-8", dtype=str, keep_default_na=False)
            df = df.rename(columns={c: CSV_TO_BRONZE[c] for c in df.columns if c in CSV_TO_BRONZE})
            cols = [c for c in df.columns if c in CSV_TO_BRONZE.values()]
            if cols:
                return df[cols].copy()
    return None


def load_tme_entites_to_bronze(context: AssetExecutionContext, pg: PostgreSQLResource) -> dict:
    """
    Charge le TME dans bronze.tme_entites_hydrogeo.
    Utilisable depuis l'asset ou depuis un op (ex. full_bootstrap).
    """
    _validate_schema_table("bronze", "tme_entites_hydrogeo")
    full_table = "bronze.tme_entites_hydrogeo"
    df: Optional[pd.DataFrame] = None
    source = "vide"

    # 1) Fichier local (prioritaire si présent)
    df = _read_local_tme_csv(context)
    if df is not None and len(df) > 0:
        source = "TME.csv (local)"

    # 2) Archive CSV nationale : fusionner tous les CSV TME/entités (Métropole + DOM, etc.)
    if df is None or len(df) == 0:
        try:
            zip_bytes = _fetch_zip(context, BDLISA_NATIONAL_CSV_URL)
            df = _find_and_read_csv_in_zip(zip_bytes, context, concat_all=True)
            if df is not None and len(df) > 0:
                source = BDLISA_NATIONAL_CSV_URL
        except Exception as e:
            context.log.warning("Zip BDLISA CSV national non utilisé : %s", e)

    # 3) Zip config (gpkg : souvent sans CSV)
    if df is None or len(df) == 0:
        cfg = _load_bdlisa_config(context)
        url = cfg.get("url") or DEFAULT_BDLISA_GPKG_URL
        if url != BDLISA_NATIONAL_CSV_URL:
            try:
                zip_bytes = _fetch_zip(context, url)
                df = _find_and_read_csv_in_zip(zip_bytes, context)
                if df is not None and len(df) > 0:
                    source = url
            except Exception as e:
                context.log.warning("Zip BDLISA (gpkg) sans CSV ou erreur : %s", e)

    if df is None or len(df) == 0:
        context.log.error("Aucune source TME trouvée — table vide, stg_tme_entites restera NULL")
        df = pd.DataFrame(columns=TME_BRONZE_COLUMNS)
    else:
        context.log.info("Source TME : %s — %s lignes", source, len(df))
        # Vérifier la présence de codes entité (lettres) et de codes Métropole (0xx...)
        code_series = df["code_eh"].astype(str).str.strip()
        entity_count = code_series.str.contains(r"[A-Za-z]", na=False).sum()
        metro_count = code_series.str.match(r"^0[0-9]{2}[A-Za-z]", na=False).sum()
        if entity_count == 0:
            context.log.warning("TME chargé sans codes entité (aucune lettre dans code_eh) — vérifier les CSV")
        if metro_count == 0:
            context.log.warning("Aucun code Métropole détecté dans TME — jointure BDLISA METRO impossible")

    # Toujours le même schéma (colonnes attendues par stg_tme_entites)
    for c in TME_BRONZE_COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[TME_BRONZE_COLUMNS]

    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            cur.execute("DROP TABLE IF EXISTS bronze.tme_entites_hydrogeo")
            col_defs = sql.SQL(", ").join(
                sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in TME_BRONZE_COLUMNS
            )
            cur.execute(
                sql.SQL("CREATE TABLE bronze.tme_entites_hydrogeo ({})").format(col_defs)
            )
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
    Source : 1) fichier TME.csv local (prioritaire, si présent) ;
            2) zip BDLISA CSV national (contient TME) ;
            3) zip config (gpkg, souvent sans CSV). Sinon table vide.
    Fait partie du job reference_data_bronze (chargement des référentiels).
    """
    return load_tme_entites_to_bronze(context, pg)
