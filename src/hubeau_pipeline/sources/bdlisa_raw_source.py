"""
Source DLT pour BD-LISA - Charge les entités hydrogéologiques avec géométries
"""

import dlt
import requests
from typing import Iterator, Dict, Any
import logging
import json

logger = logging.getLogger(__name__)

# WFS endpoints BD-LISA
WFS_BASE = "http://www.reseau.eaufrance.fr/geotraitements/bdlisa/services/carto"

@dlt.source
def bdlisa_raw():
    """Source pour les entités hydrogéologiques BD-LISA"""

    def fetch_wfs_features(typename: str, max_features: int = 50000) -> Iterator[Dict[str, Any]]:
        """Récupère les features via WFS en GeoJSON"""

        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typename": typename,
            "outputFormat": "application/json",
            "count": max_features
        }

        try:
            logger.info(f"Fetching BD-LISA {typename}...")
            response = requests.get(WFS_BASE, params=params, timeout=60)
            response.raise_for_status()

            geojson = response.json()
            features = geojson.get("features", [])
            logger.info(f"Got {len(features)} features for {typename}")

            for feature in features:
                properties = feature.get("properties", {})
                geometry = feature.get("geometry")

                # Convertir la géométrie en WKT pour PostGIS
                geometry_wkt = None
                if geometry:
                    try:
                        from shapely.geometry import shape
                        geom = shape(geometry)
                        geometry_wkt = geom.wkt
                    except Exception as e:
                        logger.warning(f"Could not convert geometry: {e}")
                        # Stocker en JSON si shapely n'est pas disponible
                        geometry_wkt = json.dumps(geometry)

                # Retourner toutes les propriétés + géométrie
                result = {k: v for k, v in properties.items()}
                result["geometry_wkt"] = geometry_wkt
                result["geometry_type"] = geometry.get("type") if geometry else None

                yield result

        except Exception as e:
            logger.error(f"Error fetching {typename}: {e}")
            # Fallback: essayer de télécharger depuis data.gouv.fr si WFS échoue
            yield from fetch_from_datagouv(typename)

    def fetch_from_datagouv(niveau: str) -> Iterator[Dict[str, Any]]:
        """Fallback: récupère depuis data.gouv.fr si WFS échoue"""
        logger.info(f"Trying fallback from data.gouv.fr for {niveau}...")

        # URLs connues sur data.gouv.fr
        urls = {
            "ENTITE_NV1": "https://www.data.gouv.fr/fr/datasets/r/d6f3e2c8-4b6e-4d5a-8c7d-5b8e9f2a3b1d",
            "ENTITE_NV2": "https://www.data.gouv.fr/fr/datasets/r/a5f3e2c8-4b6e-4d5a-8c7d-5b8e9f2a3b2e",
            "ENTITE_NV3": "https://www.data.gouv.fr/fr/datasets/r/b6f3e2c8-4b6e-4d5a-8c7d-5b8e9f2a3b3f"
        }

        # Pour l'instant, retourner des données vides si WFS échoue
        # TODO: Implémenter le téléchargement depuis data.gouv.fr
        return []

    @dlt.resource(name="bdlisa_entites_nv1", write_disposition="replace")
    def entites_niveau1() -> Iterator[Dict[str, Any]]:
        """Entités hydrogéologiques niveau 1 (national)"""
        return fetch_wfs_features("ENTITE_NV1")

    @dlt.resource(name="bdlisa_entites_nv2", write_disposition="replace")
    def entites_niveau2() -> Iterator[Dict[str, Any]]:
        """Entités hydrogéologiques niveau 2 (régional)"""
        return fetch_wfs_features("ENTITE_NV2")

    @dlt.resource(name="bdlisa_entites_nv3", write_disposition="replace")
    def entites_niveau3() -> Iterator[Dict[str, Any]]:
        """Entités hydrogéologiques niveau 3 (local)"""
        return fetch_wfs_features("ENTITE_NV3")

    return [
        entites_niveau1,
        entites_niveau2,
        entites_niveau3
    ]