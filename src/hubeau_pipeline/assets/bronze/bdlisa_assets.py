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
    import re
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


def _load_config(context: AssetExecutionContext) -> BDLISAConfig:
    """Charge la config depuis configs/bdlisa/bdlisa_entites.yml si présent."""
    import yaml
    for base in ("configs", "/app/configs"):
        path = Path(base) / "bdlisa" / "bdlisa_entites.yml"
        if path.exists():
            try:
                with open(path) as f:
                    data = yaml.safe_load(f) or {}
                r = data.get("resource", {})
                e = data.get("extraction", {})
                return BDLISAConfig(
                    url=r.get("url", DEFAULT_BDLISA_URL),
                    schema_name=e.get("schema", "bronze"),
                    table_name=e.get("table", "bdlisa_entites_raw"),
                    layer_index=e.get("layer_index", 0),
                )
            except Exception as ex:
                context.log.warning(f"Config {path} ignorée: {ex}")
    return BDLISAConfig()


def _load_gpkg_into_postgis(
    zip_bytes: bytes,
    cfg: BDLISAConfig,
    pg: PostgreSQLResource,
    context: AssetExecutionContext,
) -> dict:
    """Extrait le GeoPackage du ZIP et charge dans PostGIS (géométrie conservée)."""
    from sqlalchemy import create_engine

    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        gpkg_names = [n for n in zf.namelist() if n.lower().endswith(".gpkg")]
        if not gpkg_names:
            raise ValueError("Aucun fichier .gpkg dans le ZIP BDLISA")
        gpkg_names.sort()
        first_gpkg = gpkg_names[0]
        with zf.open(first_gpkg) as f:
            # Lire le gpkg en mémoire (GeoPandas peut lire depuis un buffer)
            buf = io.BytesIO(f.read())
    context.log.info(f"GeoPackage lu: {first_gpkg}")

    gdf = gpd.read_file(buf, layer=cfg.layer_index)
    context.log.info(f"Layer chargé: {len(gdf):,} entités, géométrie: {gdf.geometry.type.iloc[0] if len(gdf) else 'N/A'}")
    context.log.info(f"Colonnes BDLISA: {list(gdf.columns)}")

    # Normaliser noms de colonnes (minuscules, underscore) pour dbt
    gdf.columns = [_normalize_column_name(c) for c in gdf.columns]
    # S'assurer que la géométrie est en WGS84 pour cohérence avec le reste du projet
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    _validate_schema_table(cfg.schema_name, cfg.table_name)
    engine = create_engine(pg.get_dsn())
    full_table = f"{cfg.schema_name}.{cfg.table_name}"
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_name}")

    gdf.to_postgis(
        cfg.table_name,
        engine,
        schema=cfg.schema_name,
        if_exists="replace",
        index=False,
    )
    engine.dispose()
    context.log.info(f"Table {full_table} alimentée: {len(gdf):,} lignes (avec géométrie)")

    # Vue avec schéma fixe (code_eh, libelle_eh, ...) pour dbt, quel que soit le nommage BDLISA
    cols = list(gdf.columns)
    code_col = next((c for c in cols if "code" in c and ("entite" in c or c == "code")), cols[0] if cols else "code")
    libelle_col = next((c for c in cols if "libelle" in c or "lb_" in c), None) or (cols[1] if len(cols) >= 2 else None)
    niveau_col = next((c for c in cols if "niveau" in c), None)
    etat_col = next((c for c in cols if "etat" in c), None)
    nature_col = next((c for c in cols if "nature" in c), None)
    milieu_col = next((c for c in cols if "milieu" in c), None)
    theme_col = next((c for c in cols if "theme" in c), None)
    origine_col = next((c for c in cols if "origine" in c), None)
    geom_col = next((c for c in cols if c == "geometry" or "geom" in c), None)

    def _col_expr(c: Optional[str], default: str = "NULL") -> str:
        return f'NULLIF(TRIM("{c}"::text), \'\')' if c else default
    libelle_expr = f'"{libelle_col}"::text' if libelle_col else "NULL::text"
    geom_expr = f'"{geom_col}"' if geom_col else "NULL::geometry"

    view_sql = f"""
    CREATE OR REPLACE VIEW {cfg.schema_name}.bdlisa_entites AS
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
    FROM {full_table}
    WHERE TRIM(COALESCE("{code_col}"::text, '')) != '' AND TRIM(COALESCE("{code_col}"::text, '')) != 'X'
    """
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(view_sql)
        conn.commit()
    context.log.info(f"Vue {cfg.schema_name}.bdlisa_entites créée (schéma fixe pour dbt)")

    return {
        "rows_loaded": len(gdf),
        "table": full_table,
        "view": f"{cfg.schema_name}.bdlisa_entites",
        "source": first_gpkg,
        "columns": list(gdf.columns),
        "has_geometry": True,
    }


def _load_csv_fallback(
    zip_bytes: bytes,
    cfg: BDLISAConfig,
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

    _validate_schema_table(cfg.schema_name, cfg.table_name)
    full_table = f"{cfg.schema_name}.{cfg.table_name}"
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_name}")
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
    description="Référentiel BDLISA V3 (entités hydrogéologiques) chargé depuis bdlisa.eaufrance.fr — format GeoPackage pour PostGIS",
    group_name="bronze",
    compute_kind="python",
)
def bdlisa_entites_raw(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict:
    """
    Télécharge le ZIP BDLISA (GeoPackage par défaut), extrait le fichier,
    charge dans bronze.bdlisa_entites_raw avec géométrie PostGIS.
    Si le ZIP contient du CSV uniquement, charge en fallback sans géométrie.
    Config : configs/bdlisa/bdlisa_entites.yml ou valeurs par défaut.
    """
    cfg = _load_config(context)
    context.log.info(f"Téléchargement BDLISA: {cfg.url}")

    with httpx.Client(timeout=cfg.timeout_seconds) as client:
        resp = client.get(cfg.url)
        resp.raise_for_status()
        zip_bytes = resp.content
    context.log.info(f"ZIP téléchargé: {len(zip_bytes):,} octets")

    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        has_gpkg = any(n.lower().endswith(".gpkg") for n in zf.namelist())
        has_csv = any(n.lower().endswith(".csv") for n in zf.namelist())

    if has_gpkg:
        return _load_gpkg_into_postgis(zip_bytes, cfg, pg, context)
    if has_csv:
        context.log.warning("Pas de .gpkg dans le ZIP, fallback CSV (sans géométrie)")
        return _load_csv_fallback(zip_bytes, cfg, pg, context)
    raise ValueError("ZIP BDLISA sans .gpkg ni .csv")
