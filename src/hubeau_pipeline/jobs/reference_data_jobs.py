"""
Jobs Données de Référence - TME uniquement

Charge uniquement les entités hydrogéologiques TME (bronze.tme_entites_hydrogeo).
Les autres référentiels (Sandre, géo) ne sont pas activés pour l'instant.
"""

from dagster import AssetSelection, define_asset_job

from ..assets.bronze.tme_entites_assets import tme_entites_hydrogeo

reference_data_bronze_job = define_asset_job(
    name="reference_data_bronze",
    description=(
        "Charge les données de référence TME uniquement (entités hydrogéologiques). "
        "Les autres référentiels (Sandre, géo) ne sont pas activés."
    ),
    selection=AssetSelection.assets(
        tme_entites_hydrogeo,
    ),
    tags={"dagster/concurrency_key": "reference_data_bronze"},
)
