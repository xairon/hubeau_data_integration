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
    description="🌊 Hub'Eau - 8 APIs COMPLÈTES REFACTORISÉES → MinIO",
    selection=AssetSelection.keys(
        # 4 APIs Hub'Eau existantes
        "hubeau_piezo_bronze_real",                    # ✅ Piezometrie v1
        "hubeau_hydro_bronze_real",                    # ✅ Hydrometrie v2  
        "hubeau_quality_groundwater_bronze_real",       # ✅ Qualité nappes v1
        "hubeau_temperature_bronze_real",               # ✅ Température v1
        
        # 4 APIs Hub'Eau nouvellement refactorisées
        "hubeau_quality_surface_bronze_real",           # ✅ Qualité surface v2 
        "hubeau_onde_bronze_real",                      # ✅ ONDE v1
        "hubeau_hydrobiologie_bronze_real",             # ✅ Hydrobiologie v1
        "hubeau_prelevements_bronze_real"               # ✅ Prélèvements v1
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
