"""
Dagster Jobs — SoftCLT Embeddings (ML Layer)

8 jobs:
- 4 training (manual via Dagster UI, 2 domains x 2 spaces)
- 4 nightly encode + cluster (sensor-driven, 2 domains x 2 spaces)
"""

from dagster import define_asset_job, AssetSelection

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
