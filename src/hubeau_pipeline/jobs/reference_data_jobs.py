"""
Jobs Données de Référence - BDLISA + Sandre + Référentiels géo

Charge le bronze des référentiels utilisés par dbt (stg_tme_entites, etc.) et les calques Superset :
- bdlisa_entites_raw : GeoPackage BDLISA V3 → PostGIS
- tme_entites_hydrogeo : attributs TME (niveau, etat, nature, ...) depuis TME.csv
- sandre_nomenclatures_eh : nomenclatures Sandre (ref_*_eh)
- referentiel_regions, referentiel_departements, referentiel_zones_hydro : calques géographiques

À lancer avant un full_bootstrap ou avant le premier dbt run pour avoir les entités hydrogéologiques.
"""

from dagster import AssetSelection, define_asset_job

from ..assets.bronze.bdlisa_assets import bdlisa_entites_raw
from ..assets.bronze.tme_entites_assets import tme_entites_hydrogeo
from ..assets.bronze.sandre_nomenclatures_assets import sandre_nomenclatures_eh
from ..assets.bronze.referentiel_geo_assets import (
    referentiel_regions,
    referentiel_departements,
    referentiel_zones_hydro,
)

reference_data_bronze_job = define_asset_job(
    name="reference_data_bronze",
    description=(
        "Charge les données de référence Bronze : BDLISA (entités hydrogéologiques + géométrie PostGIS), "
        "nomenclatures Sandre (ref_*_eh) et référentiels géo (régions, départements, zones hydro) pour Superset."
    ),
    selection=AssetSelection.assets(
        bdlisa_entites_raw,
        tme_entites_hydrogeo,
        sandre_nomenclatures_eh,
        referentiel_regions,
        referentiel_departements,
        referentiel_zones_hydro,
    ),
    tags={"dagster/concurrency_key": "reference_data_bronze"},
)
