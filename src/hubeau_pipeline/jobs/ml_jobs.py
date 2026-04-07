"""
Dagster Jobs — ML Layer

12 jobs:
- 4 training (manual via Dagster UI, 2 domains x 2 spaces)
- 4 nightly encode + cluster (sensor-driven, 2 domains x 2 spaces)
- 1 Pastas IRF features (manual)
- 3 Pastas v3: signatures, SGI, full re-fit (manual)
"""

from dagster import AssetSelection, define_asset_job

# Training (manual — launch from Dagster UI)
ml_piezo_multi_train_job = define_asset_job(
    name="ml_piezo_multi_train_job",
    selection=AssetSelection.assets("ml_piezo_multi_model_train"),
    description="Train SoftCLT encoder for piezometry MULTI (GPU, ~15-30min)",
)
ml_piezo_uni_train_job = define_asset_job(
    name="ml_piezo_uni_train_job",
    selection=AssetSelection.assets("ml_piezo_uni_model_train"),
    description="Train SoftCLT encoder for piezometry UNI (GPU, ~15-30min)",
)
ml_hydro_multi_train_job = define_asset_job(
    name="ml_hydro_multi_train_job",
    selection=AssetSelection.assets("ml_hydro_multi_model_train"),
    description="Train SoftCLT encoder for hydrometry MULTI (GPU, ~15-30min)",
)
ml_hydro_uni_train_job = define_asset_job(
    name="ml_hydro_uni_train_job",
    selection=AssetSelection.assets("ml_hydro_uni_model_train"),
    description="Train SoftCLT encoder for hydrometry UNI (GPU, ~15-30min)",
)

# Nightly encode + cluster (sensor-driven after domain pipeline)
ml_piezo_multi_embeddings_job = define_asset_job(
    name="ml_piezo_multi_embeddings_job",
    selection=AssetSelection.assets("ml_piezo_multi_embeddings_update", "ml_piezo_multi_clusters"),
    description="Encode + cluster piezo MULTI embeddings (GPU, ~2-5min)",
)
ml_piezo_uni_embeddings_job = define_asset_job(
    name="ml_piezo_uni_embeddings_job",
    selection=AssetSelection.assets("ml_piezo_uni_embeddings_update", "ml_piezo_uni_clusters"),
    description="Encode + cluster piezo UNI embeddings (GPU, ~2-5min)",
)
ml_hydro_multi_embeddings_job = define_asset_job(
    name="ml_hydro_multi_embeddings_job",
    selection=AssetSelection.assets("ml_hydro_multi_embeddings_update", "ml_hydro_multi_clusters"),
    description="Encode + cluster hydro MULTI embeddings (GPU, ~2-5min)",
)
ml_hydro_uni_embeddings_job = define_asset_job(
    name="ml_hydro_uni_embeddings_job",
    selection=AssetSelection.assets("ml_hydro_uni_embeddings_update", "ml_hydro_uni_clusters"),
    description="Encode + cluster hydro UNI embeddings (GPU, ~2-5min)",
)

# Pastas IRF features (manual — launch from Dagster UI)
pastas_irf_features_job = define_asset_job(
    name="pastas_irf_features_job",
    selection=AssetSelection.assets("ml_piezo_pastas_irf_features"),
    description="Fit Pastas models and extract IRF features for all piezometric stations (~15-25min)",
)

# Pastas v3: groundwater signatures (manual — launch from Dagster UI)
pastas_signatures_job = define_asset_job(
    name="pastas_signatures_job",
    selection=AssetSelection.assets("ml_piezo_groundwater_signatures"),
    description="Compute 29 Pastas groundwater signatures for all eligible stations (~60min)",
)

# Pastas v3: SGI (manual — launch from Dagster UI)
pastas_sgi_job = define_asset_job(
    name="pastas_sgi_job",
    selection=AssetSelection.assets("ml_piezo_sgi"),
    description="Compute monthly SGI for all eligible stations (~15-20min)",
)

# Pastas v3: full re-fit (manual — launch from Dagster UI)
pastas_full_refit_job = define_asset_job(
    name="pastas_full_refit_job",
    selection=AssetSelection.assets("ml_piezo_pastas_full_refit"),
    description="Re-fit Pastas: decomposition + water balance + enriched metrics (~45-75min)",
)
