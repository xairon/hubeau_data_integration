"""
Jobs Données de Référence - TME uniquement (mode simplifié)

Charge le bronze minimal utilisé par dbt :
- tme_entites_hydrogeo : attributs TME (codes EH)
"""

from dagster import AssetSelection, define_asset_job

from ..assets.bronze.tme_entites_assets import tme_entites_hydrogeo

reference_data_bronze_job = define_asset_job(
    name="reference_data_bronze",
    description=(
        "Charge les données de référence Bronze minimales : TME (entités hydrogéologiques, codes EH)."
    ),
    selection=AssetSelection.assets(
        tme_entites_hydrogeo,
    ),
    tags={"dagster/concurrency_key": "reference_data_bronze"},
)
