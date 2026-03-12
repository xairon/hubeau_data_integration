"""
Dagster Jobs — SoftCLT Embeddings (ML Layer)

4 jobs:
- 2 training (manual via Dagster UI)
- 2 nightly encode + cluster (sensor-driven)
"""

from dagster import define_asset_job, AssetSelection

# Training (manual — launch from Dagster UI)
ml_piezo_train_job = define_asset_job(
    name="ml_piezo_train_job",
    selection=AssetSelection.assets("ml_piezo_model_train"),
    description="Train SoftCLT encoder for piezometry (GPU, ~15-30min)",
)
ml_hydro_train_job = define_asset_job(
    name="ml_hydro_train_job",
    selection=AssetSelection.assets("ml_hydro_model_train"),
    description="Train SoftCLT encoder for hydrometry (GPU, ~15-30min)",
)

# Nightly encode + cluster (sensor-driven after domain pipeline)
ml_piezo_embeddings_job = define_asset_job(
    name="ml_piezo_embeddings_job",
    selection=AssetSelection.assets("ml_piezo_embeddings_update", "ml_piezo_clusters"),
    description="Encode + cluster piezo embeddings (GPU, ~2-5min)",
)
ml_hydro_embeddings_job = define_asset_job(
    name="ml_hydro_embeddings_job",
    selection=AssetSelection.assets("ml_hydro_embeddings_update", "ml_hydro_clusters"),
    description="Encode + cluster hydro embeddings (GPU, ~2-5min)",
)
