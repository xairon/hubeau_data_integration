#!/usr/bin/env python3
"""
Script pour matérialiser l'asset piezometers_csv_data directement.
"""

from hubeau_pipeline.definitions import defs
from dagster import materialize

# Trouver l'asset piezometers_csv_data
piezometer_assets = [asset for asset in defs.assets if asset.key.to_user_string() == "piezometers_csv_data"]

if piezometer_assets:
    print("Asset trouvé, lancement de la matérialisation...")
    result = materialize(piezometer_assets)
    print(f"Matérialisation terminée avec succès: {result.success}")
    if result.success:
        print("Les données ont été chargées dans la table PostgreSQL 'piezometers'")
else:
    print("Asset piezometers_csv_data non trouvé")