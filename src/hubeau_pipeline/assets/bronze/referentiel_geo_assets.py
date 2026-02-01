"""
Référentiels géographiques — Calques pour Superset (régions, départements, zones hydro)

Charge les contours administratifs (data.gouv.fr) et zones hydrographiques (Sandre BD Carthage)
dans bronze pour affichage en calques dans Superset (filtres par région, département, zone hydro).
"""

import io
import re
import zipfile
from pathlib import Path
from typing import Any

import httpx
import geopandas as gpd
import pandas as pd
from dagster import AssetExecutionContext, asset
from psycopg2 import sql

from hubeau_pipeline.resources import PostgreSQLResource


# data.gouv.fr — Contours administratifs (IGN / OSM), généralisation 1000m pour cartes légères
# Source: https://www.data.gouv.fr/fr/datasets/contours-administratifs/ — l’année (2025) peut être à mettre à jour
DATAGOUV_BASE = "https://object.data.gouv.fr/contours-administratifs/2025/geojson"
REGIONS_GEOJSON_URL = f"{DATAGOUV_BASE}/regions-1000m.geojson"
DEPARTEMENTS_GEOJSON_URL = f"{DATAGOUV_BASE}/departements-1000m.geojson"

# Sandre BD Carthage — Zones hydrographiques (Shapefile métropole) ; décompressé puis lu
# Source: https://www.sandre.eaufrance.fr/atlas/srv/api/records/3639a1bc-4f56-45a2-a000-bd8238428668
ZONES_HYDRO_SHP_ZIP_URL = (
    "https://services.sandre.eaufrance.fr/telechargement/geo/ETH/BDCarthage/FXX/2017/"
    "France_metropole_entiere/SHP/ZoneHydro_FXX-shp.zip"
)
ZONES_HYDRO_TIMEOUT = 300


def _validate_schema_table(schema_name: str, table_name: str) -> None:
    """Valide schéma et table pour éviter injection SQL (alphanumerique + underscore)."""
    if not (schema_name and table_name):
        raise ValueError("schema_name et table_name requis")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", schema_name):
        raise ValueError(f"schema_name invalide: {schema_name!r}")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
        raise ValueError(f"table_name invalide: {table_name!r}")


def _geojson_to_postgis(
    url: str,
    schema_name: str,
    table_name: str,
    pg: PostgreSQLResource,
    context: AssetExecutionContext,
) -> int:
    """Télécharge un GeoJSON, charge en PostGIS (WGS84). Retourne le nombre de lignes."""
    from sqlalchemy import create_engine

    context.log.info("Téléchargement: %s", url)
    with httpx.Client(timeout=120) as client:
        resp = client.get(url)
        resp.raise_for_status()
    gdf = gpd.read_file(io.BytesIO(resp.content))
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        if gdf.crs:
            gdf = gdf.to_crs(epsg=4326)
        else:
            gdf.set_crs(epsg=4326, inplace=True)
    gdf.columns = [c.lower().strip() for c in gdf.columns]
    _validate_schema_table(schema_name, table_name)
    engine = create_engine(pg.get_dsn())
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name))
            )
    gdf.to_postgis(
        table_name,
        engine,
        schema=schema_name,
        if_exists="replace",
        index=False,
    )
    engine.dispose()
    context.log.info("  %s.%s: %s lignes", schema_name, table_name, len(gdf))
    return len(gdf)


def _shp_zip_to_postgis(
    url: str,
    schema_name: str,
    table_name: str,
    pg: PostgreSQLResource,
    context: AssetExecutionContext,
) -> int:
    """Télécharge un ZIP Shapefile, extrait en temporaire, lit le .shp, charge en PostGIS (WGS84)."""
    import tempfile
    from sqlalchemy import create_engine

    context.log.info("Téléchargement: %s", url)
    with httpx.Client(timeout=ZONES_HYDRO_TIMEOUT) as client:
        resp = client.get(url)
        resp.raise_for_status()
        zip_bytes = resp.content
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        shp_names = [n for n in zf.namelist() if n.lower().endswith(".shp")]
        if not shp_names:
            raise ValueError("Aucun .shp dans le ZIP zones hydro")
        with tempfile.TemporaryDirectory() as tmp:
            zf.extractall(tmp)
            # Trouver le .shp extrait (même nom, sous-dir possible)
            from pathlib import Path
            root = Path(tmp)
            shp_path = next(root.rglob("*.shp"), None)
            if not shp_path:
                raise ValueError("Aucun .shp trouvé après extraction")
            gdf = gpd.read_file(shp_path)
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        if gdf.crs:
            gdf = gdf.to_crs(epsg=4326)
        else:
            gdf.set_crs(epsg=4326, inplace=True)
    gdf.columns = [c.lower().strip() for c in gdf.columns]
    _validate_schema_table(schema_name, table_name)
    engine = create_engine(pg.get_dsn())
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name))
            )
    gdf.to_postgis(
        table_name,
        engine,
        schema=schema_name,
        if_exists="replace",
        index=False,
    )
    engine.dispose()
    context.log.info("  %s.%s: %s lignes", schema_name, table_name, len(gdf))
    return len(gdf)


@asset(
    description="Calque régions (contours administratifs data.gouv.fr) pour Superset",
    group_name="bronze",
    compute_kind="python",
)
def referentiel_regions(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict[str, Any]:
    """Charge les contours des régions (GeoJSON 1000m) dans bronze.referentiel_regions."""
    n = _geojson_to_postgis(
        REGIONS_GEOJSON_URL,
        "bronze",
        "referentiel_regions",
        pg,
        context,
    )
    return {"rows": n, "table": "bronze.referentiel_regions"}


@asset(
    description="Calque départements (contours administratifs data.gouv.fr) pour Superset",
    group_name="bronze",
    compute_kind="python",
)
def referentiel_departements(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict[str, Any]:
    """Charge les contours des départements (GeoJSON 1000m) dans bronze.referentiel_departements."""
    n = _geojson_to_postgis(
        DEPARTEMENTS_GEOJSON_URL,
        "bronze",
        "referentiel_departements",
        pg,
        context,
    )
    return {"rows": n, "table": "bronze.referentiel_departements"}


@asset(
    description="Calque zones hydrographiques (Sandre BD Carthage) pour Superset",
    group_name="bronze",
    compute_kind="python",
)
def referentiel_zones_hydro(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
) -> dict[str, Any]:
    """
    Charge les zones hydrographiques (Shapefile BD Carthage métropole) dans bronze.referentiel_zones_hydro.
    En cas d'échec (réseau, URL Sandre), l'asset réussit avec rows=0 et error dans les métadonnées,
    afin de ne pas bloquer le job reference_data_bronze ; le calque sera simplement vide dans Superset.
    """
    try:
        n = _shp_zip_to_postgis(
            ZONES_HYDRO_SHP_ZIP_URL,
            "bronze",
            "referentiel_zones_hydro",
            pg,
            context,
        )
        return {"rows": n, "table": "bronze.referentiel_zones_hydro"}
    except Exception as e:
        context.log.warning("Zones hydro Sandre indisponibles: %s — asset ignoré.", e)
        return {"rows": 0, "table": "bronze.referentiel_zones_hydro", "error": str(e)}
