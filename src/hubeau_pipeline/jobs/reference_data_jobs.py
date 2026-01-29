"""
Jobs Données de Référence - BDLISA + Sandre

Charge le bronze des référentiels utilisés par dbt (stg_tme_entites, etc.) :
- bdlisa_entites_raw : GeoPackage BDLISA V3 → PostGIS
- sandre_nomenclatures_eh : nomenclatures Sandre (ref_*_eh)

À lancer avant un full_bootstrap ou avant le premier dbt run pour avoir les entités hydrogéologiques.
"""

from dagster import AssetSelection, define_asset_job

from ..assets.bronze.bdlisa_assets import bdlisa_entites_raw
from ..assets.bronze.sandre_nomenclatures_assets import sandre_nomenclatures_eh

reference_data_bronze_job = define_asset_job(
    name="reference_data_bronze",
    description=(
        "Charge les données de référence Bronze : BDLISA (entités hydrogéologiques + géométrie PostGIS) "
        "et nomenclatures Sandre (ref_*_eh). Requis avant dbt pour stg_tme_entites."
    ),
    selection=AssetSelection.assets(bdlisa_entites_raw, sandre_nomenclatures_eh),
    tags={"dagster/concurrency_key": "reference_data_bronze"},
)
