"""
BDLISA Bronze Asset - Téléchargement et chargement du référentiel BDLISA V3

Charge le **GeoPackage** (ou Shapefile) BDLISA pour conserver la géométrie
dans PostGIS. Fallback CSV si format non géo demandé.

Source: https://bdlisa.eaufrance.fr/telechargement
"""

import io
import re
import zipfile
from pathlib import Path
from typing import Optional

import httpx
import pandas as pd
import geopandas as gpd
from dagster import AssetExecutionContext, asset, ConfigurableResource
from pydantic import Field

from hubeau_pipeline.resources import PostgreSQLResource


# ============================================================================
# CONFIG
# ============================================================================

# URL par défaut (réutilisée dans _load_config pour éviter accès à Field.default)
DEFAULT_BDLISA_URL = (
    "https://reseau.eaufrance.fr/geotraitements/bdlisa/files/telechargement/"
    "BDLISA_V3/BDLISA_V3_METRO-gpkg.zip"
)


def _validate_schema_table(schema_name: str, table_name: str) -> None:
    """Valide schéma et table pour éviter injection SQL (alphanumerique + underscore)."""
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", schema_name):
        raise ValueError(f"schema_name invalide: {schema_name!r}")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
        raise ValueError(f"table_name invalide: {table_name!r}")


class BDLISAConfig(ConfigurableResource):
    """Configuration pour le téléchargement BDLISA."""
    # GeoPackage par défaut (géométrie PostGIS)
    url: str = Field(
        default=DEFAULT_BDLISA_URL,
        description="URL du ZIP BDLISA (gpkg ou csv)",
    )
    schema_name: str = Field(default="bronze", description="Schéma cible")
    table_name: str = Field(default="bdlisa_entites_raw", description="Table cible")
    timeout_seconds: int = Field(default=600, description="Timeout (gpkg peut être lourd)")
    # Si True, charge le premier layer du gpkg (souvent entités NV3 ou fusion)
    layer_index: int = Field(default=0, description="Index du layer à charger (0 = premier)")


def _normalize_column_name(name: str) -> str:
    """Normalise un nom de colonne pour PostgreSQL."""
    s = str(name).strip().lower()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "col"


def _load_config(context: AssetExecutionContext) -> dict:
    """Charge la config depuis configs/bdlisa/bdlisa_entites.yml. Retourne un dict avec url, perimeters, layer_index, layer_indexes, schema_name, table_name."""
    import yaml
    for base in ("configs", "/app/configs"):
        path = Path(base) / "bdlisa" / "bdlisa_entites.yml"
        if path.exists():
            try:
                with open(path) as f:
                    data = yaml.safe_load(f) or {}
                r = data.get("resource", {})
                e = data.get("extraction", {})
                perimeters = r.get("perimeters")  # liste de {code, url} ou None
                layer_indexes = e.get("layer_indexes")  # liste [0,1,2] ou None
                return {
                    "url": r.get("url", DEFAULT_BDLISA_URL),
                    "perimeters": perimeters if isinstance(perimeters, list) and perimeters else None,
                    "schema_name": e.get("schema", "bronze"),
                    "table_name": e.get("table", "bdlisa_entites_raw"),
                    "layer_index": e.get("layer_index", 0),
                    "layer_indexes": layer_indexes if isinstance(layer_indexes, list) and layer_indexes else None,
                }
            except Exception as ex:
                context.log.warning(f"Config {path} ignorée: {ex}")
    return {
        "url": DEFAULT_BDLISA_URL,
        "perimeters": None,
        "schema_name": "bronze",
        "table_name": "bdlisa_entites_raw",
        "layer_index": 0,
        "layer_indexes": None,
    }


def _write_bdlisa_to_postgis(
    gdf: gpd.GeoDataFrame,
    schema_name: str,
    table_name: str,
    pg: PostgreSQLResource,
    context: AssetExecutionContext,
) -> dict:
    """Écrit le GeoDataFrame en table PostGIS et crée la vue bdlisa_entites (schéma fixe + perimeter/niveau_layer si présents)."""
    from sqlalchemy import create_engine

    _validate_schema_table(schema_name, table_name)
    full_table = f"{schema_name}.{table_name}"
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    engine = create_engine(pg.get_dsn())
    gdf.to_postgis(
        table_name,
        engine,
        schema=schema_name,
        if_exists="replace",
        index=False,
    )
    engine.dispose()
    context.log.info(f"Table {full_table} alimentée: {len(gdf):,} lignes (avec géométrie)")
    cols = list(gdf.columns)
    context.log.info(f"Colonnes BDLISA (normalisées): {cols}")

    # Exclure la géométrie des fallbacks (première colonne texte = code, deuxième = libellé)
    geom_col = next((c for c in cols if c == "geometry" or "geom" in c), None)
    data_cols = [c for c in cols if c != geom_col and c not in ("perimeter", "niveau_layer")]

    # Détection code : "code" + "entit" (couvre entité/entite) ou colonne nommée "code"
    code_col = next(
        (c for c in cols if "code" in c and ("entit" in c or c == "code")),
        data_cols[0] if data_cols else (cols[0] if cols else "code"),
    )
    # Détection libellé : "libell" (couvre libellé/libelle) ou "lb_"
    libelle_col = next((c for c in cols if "libell" in c or "lb_" in c), None) or (
        data_cols[1] if len(data_cols) >= 2 else (cols[1] if len(cols) >= 2 else None)
    )
    niveau_col = next((c for c in cols if "niveau" in c and c != "niveau_layer"), None)
    etat_col = next((c for c in cols if "etat" in c), None)
    nature_col = next((c for c in cols if "nature" in c), None)
    milieu_col = next((c for c in cols if "milieu" in c), None)
    theme_col = next((c for c in cols if "theme" in c), None)
    origine_col = next((c for c in cols if "origine" in c), None)

    context.log.info(
        "Mapping TME: code_eh=%s, libelle_eh=%s, niveau_eh=%s, etat_eh=%s, nature_eh=%s, "
        "milieu_eh=%s, theme_eh=%s, origine_eh=%s (NULL = colonne absente du gpkg)",
        code_col, libelle_col, niveau_col, etat_col, nature_col, milieu_col, theme_col, origine_col,
    )

    def _col_expr(c: Optional[str], default: str = "NULL") -> str:
        return f'NULLIF(TRIM("{c}"::text), \'\')' if c else default
    libelle_expr = f'"{libelle_col}"::text' if libelle_col else "NULL::text"
    geom_expr = f'"{geom_col}"' if geom_col else "NULL::geometry"

    extra_select = []
    if "perimeter" in cols:
        extra_select.append('"perimeter"::text AS perimeter')
    if "niveau_layer" in cols:
        extra_select.append('"niveau_layer"::int AS niveau_layer')
    extra_sql = ", " + ", ".join(extra_select) if extra_select else ""

    view_sql = f"""
    CREATE OR REPLACE VIEW {schema_name}.bdlisa_entites AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY "{code_col}"::text) AS tme_id,
        "{code_col}"::text AS code_eh,
        {libelle_expr} AS libelle_eh,
        {_col_expr(niveau_col)} AS niveau_eh,
        {_col_expr(etat_col)} AS etat_eh,
        {_col_expr(nature_col)} AS nature_eh,
        {_col_expr(milieu_col)} AS milieu_eh,
        {_col_expr(theme_col)} AS theme_eh,
        {_col_expr(origine_col)} AS origine_eh,
        {geom_expr} AS geometry
        {extra_sql}
    FROM {full_table}
    WHERE TRIM(COALESCE("{code_col}"::text, '')) != '' AND TRIM(COALESCE("{code_col}"::text, '')) != 'X'
    """
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(view_sql)
        conn.commit()
    context.log.info(f"Vue {schema_name}.bdlisa_entites créée (schéma fixe pour dbt)")
    return {
        "rows_loaded": len(gdf),
        "table": full_table,
        "view": f"{schema_name}.bdlisa_entites",
        "columns": list(gdf.columns),
        "has_geometry": True,
    }


def _load_gpkg_layers(
    zip_bytes: bytes,
    cfg: dict,
    perimeter_code: Optional[str] = None,
) -> gpd.GeoDataFrame:
    """
    Lit un ou plusieurs layers du ZIP gpkg, ajoute perimeter/niveau_layer, retourne un seul gdf (sans écrire en base).

    Prod: on extrait le .gpkg dans un fichier temporaire et on lit depuis un chemin.
    La lecture depuis un buffer en mémoire peut déclencher des erreurs SQLite/OGR
    (ex. "database disk image is malformed") selon les builds GDAL/pyogrio.
    """
    import tempfile

    indices = cfg.get("layer_indexes") or [cfg.get("layer_index", 0)]
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        gpkg_names = [n for n in zf.namelist() if n.lower().endswith(".gpkg")]
        if not gpkg_names:
            raise ValueError("Aucun fichier .gpkg dans le ZIP BDLISA")
        gpkg_name = gpkg_names[0]

        with tempfile.TemporaryDirectory() as tmp:
            zf.extract(gpkg_name, tmp)
            gpkg_path = Path(tmp) / gpkg_name
            if not gpkg_path.exists():
                gpkg_path = next(Path(tmp).rglob("*.gpkg"), None)
                if not gpkg_path:
                    raise ValueError("Fichier .gpkg introuvable après extraction")

            gdfs = []
            for idx in indices:
                gdf = gpd.read_file(gpkg_path, layer=idx)
                gdf.columns = [_normalize_column_name(c) for c in gdf.columns]
                if gdf.crs and gdf.crs.to_epsg() != 4326:
                    gdf = gdf.to_crs(epsg=4326)
                if perimeter_code is not None:
                    gdf["perimeter"] = perimeter_code
                if len(indices) > 1:
                    gdf["niveau_layer"] = idx
                gdfs.append(gdf)

            return pd.concat(gdfs, ignore_index=True) if len(gdfs) > 1 else gdfs[0]


def _load_csv_fallback(
    zip_bytes: bytes,
    cfg: dict,
    pg: PostgreSQLResource,
    context: AssetExecutionContext,
) -> dict:
    """Fallback: charge le CSV du ZIP (sans géométrie)."""
    from psycopg2.extras import execute_values

    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError("Aucun fichier .csv dans le ZIP BDLISA")
        csv_names.sort(key=lambda n: (-len(n), n))
        first_csv = csv_names[0]
        with zf.open(first_csv) as f:
            df = pd.read_csv(f, encoding="utf-8", on_bad_lines="warn", low_memory=False)
    context.log.info(f"CSV lu: {first_csv} -> {len(df):,} lignes")

    df.columns = [_normalize_column_name(c) for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(object).where(pd.notna(df[c]), None)

    _validate_schema_table(cfg["schema_name"], cfg["table_name"])
    full_table = f"{cfg['schema_name']}.{cfg['table_name']}"
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg['schema_name']}")
            cols = list(df.columns)
            col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
            cur.execute(f'DROP TABLE IF EXISTS {full_table}')
            cur.execute(f'CREATE TABLE {full_table} ({col_defs})')
            conn.commit()
        batch_size = 5000
        rows_inserted = 0
        with conn.cursor() as cur:
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i : i + batch_size]
                values = [tuple(row) for row in batch.to_numpy().tolist()]
                execute_values(
                    cur,
                    f'INSERT INTO {full_table} ({",".join(chr(34) + c + chr(34) for c in cols)}) VALUES %s',
                    values,
                )
                conn.commit()
                rows_inserted += len(batch)
    return {
        "rows_loaded": rows_inserted,
        "table": full_table,
        "source": first_csv,
        "columns": list(df.columns),
        "has_geometry": False,
    }


@asset(
    description="Référentiel BDLISA V3 (entités hydrogéologiques) — multi-périmètres, multi-niveaux (NV1/NV2/NV3) pour Superset",
    group_name="bronze",
    compute_kind="python",
)
def bdlisa_entites_raw(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    """
    Télécharge un ou plusieurs ZIP BDLISA (GeoPackage), charge dans bronze.bdlisa_entites_raw
    avec géométrie PostGIS. Colonnes optionnelles : perimeter (ex. METRO, ARA), niveau_layer (0,1,2).
    Config : configs/bdlisa/bdlisa_entites.yml — perimeters (liste {code, url}), layer_indexes ([0,1,2]).
    """
    cfg = _load_config(context)
    timeout = 600
    perimeters = cfg.get("perimeters")
    if perimeters:
        items = [(p.get("code", "METRO"), p.get("url")) for p in perimeters if p.get("url")]
    else:
        items = [("METRO", cfg["url"])]

    all_gdfs: list = []
    with httpx.Client(timeout=timeout) as client:
        for perimeter_code, url in items:
            attempts = 2
            last_err: Optional[Exception] = None

            for attempt in range(1, attempts + 1):
                try:
                    context.log.info(f"Téléchargement BDLISA: {perimeter_code} — {url} (tentative {attempt}/{attempts})")
                    resp = client.get(url)
                    resp.raise_for_status()
                    zip_bytes = resp.content

                    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
                        has_gpkg = any(n.lower().endswith(".gpkg") for n in zf.namelist())
                        has_csv = any(n.lower().endswith(".csv") for n in zf.namelist())

                    if has_gpkg:
                        gdf = _load_gpkg_layers(zip_bytes, cfg, perimeter_code=perimeter_code)
                        all_gdfs.append(gdf)
                        last_err = None
                        break

                    if has_csv and len(items) == 1:
                        context.log.warning("Pas de .gpkg dans le ZIP, fallback CSV (sans géométrie)")
                        return _load_csv_fallback(zip_bytes, cfg, pg, context)

                    if has_csv:
                        context.log.warning(f"Périmètre {perimeter_code}: pas de .gpkg, ignoré")
                        last_err = None
                        break

                    raise ValueError("ZIP BDLISA sans .gpkg ni .csv")

                except zipfile.BadZipFile as e:
                    last_err = e
                    context.log.warning("ZIP BDLISA invalide (download tronqué ?) : %s", e)
                except Exception as e:
                    last_err = e
                    context.log.warning("Erreur lecture BDLISA (%s): %s", type(e).__name__, e)

            if last_err is not None:
                raise last_err

    if not all_gdfs:
        raise ValueError("Aucun GeoPackage BDLISA chargé")

    big_gdf = pd.concat(all_gdfs, ignore_index=True)
    context.log.info(f"Total BDLISA: {len(big_gdf):,} lignes, périmètres: {[c for c, _ in items]}")
    return _write_bdlisa_to_postgis(
        big_gdf,
        cfg["schema_name"],
        cfg["table_name"],
        pg,
        context,
    )
