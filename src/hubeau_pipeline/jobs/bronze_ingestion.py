"""
Jobs Bronze Hub'Eau - Architecture moderne et claire
Jobs correspondant à la nouvelle structure des assets
"""

from dagster import AssetSelection, define_asset_job

# ================================
# JOB 1 : HUB'EAU BRONZE COMPLET (sans Hydrométrie)
# ================================
# Note: Hydrométrie exclu car partitions différentes (30 jours vs 3 ans)

hubeau_bronze_job = define_asset_job(
    name="hubeau_bronze_job",
    description="🌊 Hub'Eau Bronze - APIs avec partitions quotidiennes",
    selection=AssetSelection.assets(
        # "hubeau_hydrometry_bronze",   # ❌ Exclu - partitions 30 jours (restriction API)
        # "hubeau_prelevements_bronze",  # ❌ Exclu - partitions annuelles (données annuelles)
        "hubeau_piezometry_bronze",
        "hubeau_water_quality_surface_bronze",
        "hubeau_water_quality_groundwater_bronze",
        "hubeau_temperature_bronze",
        "hubeau_onde_bronze",
        "hubeau_hydrobiology_bronze",
        "hubeau_ingestion_summary",
    ),
)

# ================================
# JOB 1B : HYDROMÉTRIE (30 jours seulement)
# ================================

hubeau_hydrometry_job = define_asset_job(
    name="hubeau_hydrometry_job",
    description="🌊 Hydrométrie Hub'Eau - ⚠️ 30 derniers jours uniquement (restriction API v2)",
    selection=AssetSelection.assets("hubeau_hydrometry_bronze"),
)

# ================================
# JOB 2 : HUB'EAU HYDROLOGIE (Piézométrie uniquement)
# ================================
# Note: Hydrométrie a son propre job (partitions incompatibles)

hubeau_hydrology_job = define_asset_job(
    name="hubeau_hydrology_job",
    description="🏔️ Hub'Eau Hydrologie - Piézométrie uniquement",
    selection=AssetSelection.assets("hubeau_piezometry_bronze"),
)

# ================================
# JOB 3 : HUB'EAU QUALITÉ EAU
# ================================

hubeau_water_quality_job = define_asset_job(
    name="hubeau_water_quality_job",
    description="🧪 Hub'Eau Qualité Eau - Cours d'eau et Nappes",
    selection=AssetSelection.assets(
        "hubeau_water_quality_surface_bronze",
        "hubeau_water_quality_groundwater_bronze",
    ),
)

# ================================
# JOB 4 : HUB'EAU ENVIRONNEMENT
# ================================

hubeau_environment_job = define_asset_job(
    name="hubeau_environment_job",
    description="🌡️ Hub'Eau Environnement - Température, ONDE, Hydrobiologie",
    selection=AssetSelection.assets(
        "hubeau_temperature_bronze",
        "hubeau_onde_bronze",
        "hubeau_hydrobiology_bronze",
    ),
)

# ================================
# JOB 5 : HUB'EAU PRÉLÈVEMENTS
# ================================

hubeau_prelevements_job = define_asset_job(
    name="hubeau_prelevements_job",
    description="💧 Hub'Eau Prélèvements - Chroniques de prélèvements",
    selection=AssetSelection.assets("hubeau_prelevements_bronze"),
)

# ================================
# JOB 6 : HUB'EAU SYNTHÈSE
# ================================

hubeau_summary_job = define_asset_job(
    name="hubeau_summary_job",
    description="📊 Hub'Eau Synthèse - Métriques globales",
    selection=AssetSelection.assets("hubeau_ingestion_summary"),
)

# ================================
# JOBS EXTERNES (LEGACY)
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

# Jobs Hub'Eau
hubeau_jobs = [
    hubeau_bronze_job,           # Toutes APIs sauf Hydrométrie
    hubeau_hydrometry_job,       # Hydrométrie seule (30 jours max)
    hubeau_hydrology_job,        # Piézométrie uniquement
    hubeau_water_quality_job,    # Qualité Eau
    hubeau_environment_job,      # Température, ONDE, Hydrobiologie
    hubeau_prelevements_job,     # Prélèvements
    hubeau_summary_job           # Synthèse
]

# Jobs externes (legacy)
external_jobs = [
    bdlisa_bronze_job,
    sandre_bronze_job
]

# Tous les jobs
all_jobs = hubeau_jobs + external_jobs