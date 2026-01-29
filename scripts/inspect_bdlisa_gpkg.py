#!/usr/bin/env python3
"""
Inspecte les colonnes du GeoPackage BDLISA V3 (layer 0 par défaut).

Usage:
  python scripts/inspect_bdlisa_gpkg.py
  python scripts/inspect_bdlisa_gpkg.py --layer 1
  python scripts/inspect_bdlisa_gpkg.py --list-layers

Dépendances: geopandas, httpx (ou environnement projet avec uv run / venv).

Affiche les colonnes brutes, normalisées, et le mapping TME (code_eh, libelle_eh,
niveau_eh, etc.). Utile pour vérifier pourquoi niveau/etat/nature/milieu/theme/origine
sont NULL dans stg_tme_entites.
"""

from __future__ import annotations

import argparse
import io
import re
import sys
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import httpx

DEFAULT_URL = (
    "https://reseau.eaufrance.fr/geotraitements/bdlisa/files/telechargement/"
    "BDLISA_V3/BDLISA_V3_METRO-gpkg.zip"
)


def _normalize(name: str) -> str:
    s = str(name).strip().lower()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "col"


def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect BDLISA GeoPackage columns")
    ap.add_argument("--url", default=DEFAULT_URL, help="URL du ZIP BDLISA")
    ap.add_argument("--layer", type=int, default=0, help="Index du layer (0 par défaut)")
    ap.add_argument("--list-layers", action="store_true", help="Lister les layers du gpkg")
    ap.add_argument("--timeout", type=int, default=300)
    args = ap.parse_args()

    print(f"Téléchargement: {args.url}")
    r = httpx.get(args.url, timeout=args.timeout)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content), "r")
    names = [n for n in z.namelist() if n.lower().endswith(".gpkg")]
    if not names:
        print("Aucun .gpkg dans le ZIP")
        return 1
    gpkg_name = names[0]

    with tempfile.TemporaryDirectory() as tmp:
        z.extract(gpkg_name, tmp)
        path = Path(tmp) / gpkg_name
        if not path.exists():
            path = next(Path(tmp).rglob("*.gpkg"), None)
        if not path:
            print("Fichier .gpkg introuvable")
            return 1

        if args.list_layers:
            layers = gpd.list_layers(str(path))
            print("Layers:")
            print(layers.to_string())
            return 0

        gdf = gpd.read_file(path, layer=args.layer)
        raw = list(gdf.columns)
        normalized = [_normalize(c) for c in raw]
        gdf.columns = normalized

    print("\n--- Colonnes brutes (gpkg) ---")
    for c in raw:
        print(f"  {c!r}")
    print("\n--- Colonnes normalisées (vers bdlisa_entites) ---")
    for c in normalized:
        print(f"  {c}")

    cols = normalized
    code_col = next((c for c in cols if "code" in c and ("entite" in c or c == "code")), cols[0] if cols else None)
    libelle_col = next((c for c in cols if "libelle" in c or "lb_" in c), None) or (cols[1] if len(cols) >= 2 else None)
    niveau_col = next((c for c in cols if "niveau" in c and c != "niveau_layer"), None)
    etat_col = next((c for c in cols if "etat" in c), None)
    nature_col = next((c for c in cols if "nature" in c), None)
    milieu_col = next((c for c in cols if "milieu" in c), None)
    theme_col = next((c for c in cols if "theme" in c), None)
    origine_col = next((c for c in cols if "origine" in c), None)

    print("\n--- Mapping TME (vue bdlisa_entites) ---")
    print(f"  code_eh     <- {code_col}")
    print(f"  libelle_eh  <- {libelle_col}")
    print(f"  niveau_eh   <- {niveau_col or '(absent)'}")
    print(f"  etat_eh     <- {etat_col or '(absent)'}")
    print(f"  nature_eh   <- {nature_col or '(absent)'}")
    print(f"  milieu_eh   <- {milieu_col or '(absent)'}")
    print(f"  theme_eh    <- {theme_col or '(absent)'}")
    print(f"  origine_eh  <- {origine_col or '(absent)'}")

    if code_col and libelle_col:
        print("\n--- Aperçu (3 lignes) ---")
        print(gdf[[code_col, libelle_col]].head(3).to_string())

    return 0


if __name__ == "__main__":
    sys.exit(main())
