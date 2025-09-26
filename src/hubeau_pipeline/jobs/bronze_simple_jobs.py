"""
Jobs Bronze Essentiels - 3 jobs seulement
Organisation simple et claire selon les sources de données
"""

from dagster import define_asset_job, AssetSelection

# ================================
# JOB 1 : HUB'EAU (8 APIs)
# ================================

hubeau_bronze_job = define_asset_job(
    name="hubeau_bronze_job",
    description="🌊 Hub'Eau - 4 APIs FONCTIONNELLES → MinIO",
    selection=AssetSelection.keys(
        # 4 APIs Hub'Eau QUI MARCHENT RÉELLEMENT
        "hubeau_piezo_bronze_real",                    # ✅ 20k stations + 843 chroniques
        "hubeau_hydro_bronze_real",                    # ✅ 2718 observations  
        "hubeau_quality_groundwater_bronze_real",       # ✅ 20k stations + 20k analyses
        "hubeau_temperature_bronze_real",               # ✅ 849 stations + 835 chroniques
        
        # APIs non disponibles (documentation seulement)
        "hubeau_apis_unavailable_bronze_real"          # 📋 Documentation des APIs non disponibles
    )
)

# ================================
# JOB 2 : BDLISA (Géologie)
# ================================

bdlisa_bronze_job = define_asset_job(
    name="bdlisa_bronze_job", 
    description="🗺️ BDLISA - Géologie WFS → MinIO",
    selection=AssetSelection.keys(
        "bdlisa_geographic_bronze_real"
    )
)

# ================================
# JOB 3 : SANDRE (Nomenclatures)
# ================================

sandre_bronze_job = define_asset_job(
    name="sandre_bronze_job",
    description="📚 Sandre - Nomenclatures APIs → MinIO", 
    selection=AssetSelection.keys(
        "sandre_thesaurus_bronze_real"
    )
)
