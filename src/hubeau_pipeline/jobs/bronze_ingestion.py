"""
Jobs Bronze Hub'Eau - Architecture simple : 1 job par API
"""

from dagster import AssetSelection, define_asset_job

# ================================
# JOBS HUB'EAU - 1 JOB PAR API
# ================================

hubeau_hydrometry_job = define_asset_job(
    name="hubeau_hydrometry_job",
    description="🌊 Hydrométrie - ⚠️ 30 derniers jours uniquement (restriction API v2)",
    selection=AssetSelection.assets("hubeau_hydrometry_bronze"),
)

hubeau_piezometry_job = define_asset_job(
    name="hubeau_piezometry_job",
    description="🏔️ Piézométrie - Niveaux des nappes phréatiques",
    selection=AssetSelection.assets("hubeau_piezometry_bronze"),
)

hubeau_temperature_job = define_asset_job(
    name="hubeau_temperature_job",
    description="🌡️ Température - Température des cours d'eau",
    selection=AssetSelection.assets("hubeau_temperature_bronze"),
)

hubeau_water_quality_surface_job = define_asset_job(
    name="hubeau_water_quality_surface_job",
    description="🧪 Qualité Cours d'Eau - Analyses physico-chimiques",
    selection=AssetSelection.assets("hubeau_water_quality_surface_bronze"),
)

hubeau_water_quality_groundwater_job = define_asset_job(
    name="hubeau_water_quality_groundwater_job",
    description="🧪 Qualité Nappes - Analyses physico-chimiques",
    selection=AssetSelection.assets("hubeau_water_quality_groundwater_bronze"),
)

hubeau_ecoulement_job = define_asset_job(
    name="hubeau_ecoulement_job",
    description="🌊 Écoulement - Observatoire National Des Étiages",
    selection=AssetSelection.assets("hubeau_ecoulement_bronze"),
)

hubeau_hydrobiology_job = define_asset_job(
    name="hubeau_hydrobiology_job",
    description="🐟 Hydrobiologie - Indices biologiques",
    selection=AssetSelection.assets("hubeau_hydrobiology_bronze"),
)

hubeau_hydrobio_taxons_job = define_asset_job(
    name="hubeau_hydrobio_taxons_job",
    description="🐟 Hydrobiologie Taxons - Ingestion générique dlt",
    selection=AssetSelection.assets("hydrobio_taxons"),
)

hubeau_prelevements_job = define_asset_job(
    name="hubeau_prelevements_job",
    description="💧 Prélèvements - Volumes annuels de prélèvements",
    selection=AssetSelection.assets("hubeau_prelevements_bronze"),
)

# ================================
# JOBS EXTERNES
# ================================

bdlisa_bronze_job = define_asset_job(
    name="bdlisa_bronze_job",
    description="🏔️ BDLISA - Géologie et hydrogéologie",
    selection=AssetSelection.assets("bdlisa_geographic_bronze_real"),
)

sandre_bronze_job = define_asset_job(
    name="sandre_bronze_job",
    description="📚 Sandre - Nomenclatures et thésaurus",
    selection=AssetSelection.assets("sandre_thesaurus_bronze_real"),
)

# ================================
# EXPORTS
# ================================

# Jobs Hub'Eau (1 par API)
hubeau_jobs = [
    hubeau_hydrometry_job,
    hubeau_piezometry_job,
    hubeau_temperature_job,
    hubeau_water_quality_surface_job,
    hubeau_water_quality_groundwater_job,
    hubeau_ecoulement_job,
    hubeau_hydrobiology_job,
    hubeau_hydrobio_taxons_job,
    hubeau_prelevements_job,
]

# Jobs externes
external_jobs = [
    bdlisa_bronze_job,
    sandre_bronze_job,
]

# Tous les jobs
all_jobs = hubeau_jobs + external_jobs