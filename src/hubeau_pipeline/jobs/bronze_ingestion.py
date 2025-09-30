"""
Jobs Bronze Hub'Eau - Architecture moderne et claire
Jobs correspondant à la nouvelle structure des assets
"""

from dagster import define_asset_job, AssetSelection

# ================================
# JOB 1 : HUB'EAU BRONZE COMPLET
# ================================

hubeau_bronze_job = define_asset_job(
    name="hubeau_bronze_job",
    description="🌊 Hub'Eau Bronze - Toutes les APIs Hub'Eau",
    selection=AssetSelection.keys(
        "hubeau_hydrometry_bronze",
        "hubeau_piezometry_bronze", 
        "hubeau_water_quality_surface_bronze",
        "hubeau_water_quality_groundwater_bronze",
        "hubeau_temperature_bronze",
        "hubeau_onde_bronze",
        "hubeau_hydrobiology_bronze",
        "hubeau_prelevements_bronze",
        "hubeau_ingestion_summary"
    )
)

# ================================
# JOB 2 : HUB'EAU HYDROLOGIE
# ================================

hubeau_hydrology_job = define_asset_job(
    name="hubeau_hydrology_job",
    description="🌊 Hub'Eau Hydrologie - Hydrométrie et Piézométrie",
    selection=AssetSelection.keys(
        "hubeau_hydrometry_bronze",
        "hubeau_piezometry_bronze"
    )
)

# ================================
# JOB 3 : HUB'EAU QUALITÉ EAU
# ================================

hubeau_water_quality_job = define_asset_job(
    name="hubeau_water_quality_job",
    description="🧪 Hub'Eau Qualité Eau - Cours d'eau et Nappes",
    selection=AssetSelection.keys(
        "hubeau_water_quality_surface_bronze",
        "hubeau_water_quality_groundwater_bronze"
    )
)

# ================================
# JOB 4 : HUB'EAU ENVIRONNEMENT
# ================================

hubeau_environment_job = define_asset_job(
    name="hubeau_environment_job",
    description="🌡️ Hub'Eau Environnement - Température, ONDE, Hydrobiologie",
    selection=AssetSelection.keys(
        "hubeau_temperature_bronze",
        "hubeau_onde_bronze",
        "hubeau_hydrobiology_bronze"
    )
)

# ================================
# JOB 5 : HUB'EAU PRÉLÈVEMENTS
# ================================

hubeau_prelevements_job = define_asset_job(
    name="hubeau_prelevements_job",
    description="💧 Hub'Eau Prélèvements - Chroniques de prélèvements",
    selection=AssetSelection.keys(
        "hubeau_prelevements_bronze"
    )
)

# ================================
# JOB 6 : HUB'EAU SYNTHÈSE
# ================================

hubeau_summary_job = define_asset_job(
    name="hubeau_summary_job",
    description="📊 Hub'Eau Synthèse - Métriques globales",
    selection=AssetSelection.keys(
        "hubeau_ingestion_summary"
    )
)

# ================================
# JOBS EXTERNES (LEGACY)
# ================================

bdlisa_bronze_job = define_asset_job(
    name="bdlisa_bronze_job",
    description="🏔️ BDLISA - Géologie et hydrogéologie",
    selection=AssetSelection.keys(
        "bdlisa_geographic_bronze_real"
    )
)

sandre_bronze_job = define_asset_job(
    name="sandre_bronze_job", 
    description="📚 Sandre - Nomenclatures et thésaurus",
    selection=AssetSelection.keys(
        "sandre_thesaurus_bronze_real"
    )
)

# ================================
# EXPORTS
# ================================

# Jobs Hub'Eau
hubeau_jobs = [
    hubeau_bronze_job,
    hubeau_hydrology_job,
    hubeau_water_quality_job,
    hubeau_environment_job,
    hubeau_prelevements_job,
    hubeau_summary_job
]

# Jobs externes (legacy)
external_jobs = [
    bdlisa_bronze_job,
    sandre_bronze_job
]

# Tous les jobs
all_jobs = hubeau_jobs + external_jobs