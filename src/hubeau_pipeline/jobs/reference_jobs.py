"""
Jobs pour charger les référentiels SANDRE et BD-LISA
"""

from dagster import define_asset_job, AssetSelection


# Job SANDRE - Charge tous les référentiels SANDRE
sandre_full_load_job = define_asset_job(
    name="sandre_full_load",
    selection=AssetSelection.groups("sandre_references", "sandre_territories", "sandre_hydro", "sandre_organizations"),
    description="Charge tous les référentiels SANDRE (14 tables)",
    tags={
        "type": "reference_data",
        "source": "sandre",
        "frequency": "weekly"
    }
)

# Job BD-LISA - Charge les entités hydrogéologiques avec géométries
bdlisa_spatial_load_job = define_asset_job(
    name="bdlisa_spatial_load",
    selection=AssetSelection.groups("bdlisa_spatial"),
    description="Charge les entités hydrogéologiques BD-LISA (3 niveaux avec géométries)",
    tags={
        "type": "spatial_data",
        "source": "bdlisa",
        "frequency": "monthly"
    }
)

# Job combiné pour charger SANDRE et BD-LISA en une fois
reference_data_full_load_job = define_asset_job(
    name="reference_data_full_load",
    selection=AssetSelection.groups(
        "sandre_references",
        "sandre_territories",
        "sandre_hydro",
        "sandre_organizations",
        "bdlisa_spatial"
    ),
    description="Charge tous les référentiels SANDRE et BD-LISA",
    tags={
        "type": "reference_data",
        "source": "sandre_bdlisa",
        "frequency": "monthly"
    }
)