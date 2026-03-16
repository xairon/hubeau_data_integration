"""
Dagster Assets — SoftCLT Embeddings (ML Layer)

6 assets in 2 groups (piezo + hydro):
- 2 training assets (manual, GPU ~15-30min)
- 2 encoding assets (nightly sensor-driven, GPU ~2-5min)
- 2 clustering assets (after encoding, CPU ~30s)
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import torch
from dagster import asset, AssetExecutionContext, MetadataValue
from sklearn.preprocessing import StandardScaler

from ..resources import PostgreSQLResource

logger = logging.getLogger(__name__)

MODELS_DIR = Path("/var/ml/models")
WINDOW_SIZE = 365
STRIDE = 90
MIN_DAYS = 540  # ~1.5 years, guarantees >=2 windows of 365d
N_EPOCHS = 200
BATCH_SIZE = 128  # A6000 48GB VRAM can handle large batches with hidden=64
EMBEDDING_DIM = 320
HIDDEN_DIM = 64  # TS2Vec/SoftCLT paper default (NOT output_dims)
DEPTH = 10
EARLY_STOP_PATIENCE = 20  # Stop if no improvement for 20 epochs


# ======================================================================
# TRAINING — Manual, GPU (~15-30min per domain)
# ======================================================================

@asset(
    group_name="ml_piezo",
    deps=["hubeau_daily_chroniques"],
    description="Train SoftCLT encoder for piezometry (~4,200 stations, GPU)",
)
def ml_piezo_model_train(context: AssetExecutionContext, pg: PostgreSQLResource):
    from ..ml.latent_space.encoder import SoftCLTEncoder
    from ..ml.latent_space.data import load_piezo_series

    series_dict, _ = load_piezo_series(pg, min_days=MIN_DAYS)
    context.log.info(f"{len(series_dict)} eligible piezo stations")

    if torch.cuda.is_available():
        gpu = torch.cuda.get_device_properties(0)
        context.log.info(f"GPU: {gpu.name}, VRAM: {gpu.total_memory / 1e9:.1f} GB")

    all_data = np.concatenate(list(series_dict.values()))
    scaler = StandardScaler().fit(all_data)
    scaled = [scaler.transform(arr).astype(np.float32) for arr in series_dict.values()]

    t0 = time.time()
    encoder = SoftCLTEncoder(input_dims=4, embedding_dim=EMBEDDING_DIM, hidden_dim=HIDDEN_DIM, depth=DEPTH)
    encoder.fit(scaled, n_epochs=N_EPOCHS, lr=1e-3, batch_size=BATCH_SIZE, early_stop_patience=EARLY_STOP_PATIENCE, dagster_context=context)
    train_duration = time.time() - t0
    context.log.info(f"Training complete in {train_duration:.0f}s ({train_duration/60:.1f} min)")

    version = f"piezo_{datetime.now():%Y%m%d_%H%M}"
    path = MODELS_DIR / version
    path.mkdir(parents=True, exist_ok=True)
    encoder.save(path / "model.pt")
    joblib.dump(scaler, path / "scaler.pkl")
    json.dump(list(series_dict.keys()), (path / "stations.json").open("w"))
    (MODELS_DIR / "piezo_latest").write_text(version)

    context.add_output_metadata({
        "model_version": version,
        "n_stations": len(series_dict),
        "device": encoder.device,
        "embedding_dim": EMBEDDING_DIM,
        "train_duration_sec": MetadataValue.float(train_duration),
    })


@asset(
    group_name="ml_hydro",
    deps=["hydro_daily_chroniques"],
    description="Train SoftCLT encoder for hydrometry (~4,200 stations, GPU)",
)
def ml_hydro_model_train(context: AssetExecutionContext, pg: PostgreSQLResource):
    from ..ml.latent_space.encoder import SoftCLTEncoder
    from ..ml.latent_space.data import load_hydro_series

    series_dict, _ = load_hydro_series(pg, min_days=MIN_DAYS)
    context.log.info(f"{len(series_dict)} eligible hydro stations")

    if torch.cuda.is_available():
        gpu = torch.cuda.get_device_properties(0)
        context.log.info(f"GPU: {gpu.name}, VRAM: {gpu.total_memory / 1e9:.1f} GB")

    all_data = np.concatenate(list(series_dict.values()))
    scaler = StandardScaler().fit(all_data)
    scaled = [scaler.transform(arr).astype(np.float32) for arr in series_dict.values()]

    t0 = time.time()
    encoder = SoftCLTEncoder(input_dims=4, embedding_dim=EMBEDDING_DIM, hidden_dim=HIDDEN_DIM, depth=DEPTH)
    encoder.fit(scaled, n_epochs=N_EPOCHS, lr=1e-3, batch_size=BATCH_SIZE, early_stop_patience=EARLY_STOP_PATIENCE, dagster_context=context)
    train_duration = time.time() - t0
    context.log.info(f"Training complete in {train_duration:.0f}s ({train_duration/60:.1f} min)")

    version = f"hydro_{datetime.now():%Y%m%d_%H%M}"
    path = MODELS_DIR / version
    path.mkdir(parents=True, exist_ok=True)
    encoder.save(path / "model.pt")
    joblib.dump(scaler, path / "scaler.pkl")
    json.dump(list(series_dict.keys()), (path / "stations.json").open("w"))
    (MODELS_DIR / "hydro_latest").write_text(version)

    context.add_output_metadata({
        "model_version": version,
        "n_stations": len(series_dict),
        "device": encoder.device,
        "embedding_dim": EMBEDDING_DIM,
        "train_duration_sec": MetadataValue.float(train_duration),
    })


# ======================================================================
# NIGHTLY ENCODE — Sensor-driven, GPU (~2-5min per domain)
# ======================================================================

@asset(
    group_name="ml_piezo",
    deps=["hubeau_daily_chroniques"],
    description="Nightly: encode piezo stations → windows + station embeddings",
)
def ml_piezo_embeddings_update(context: AssetExecutionContext, pg: PostgreSQLResource):
    from ..ml.latent_space.encoder import SoftCLTEncoder
    from ..ml.latent_space.data import load_piezo_series
    from ..ml.latent_space.persistence import init_ml_schema, upsert_station_embeddings, upsert_window_embeddings

    latest_file = MODELS_DIR / "piezo_latest"
    if not latest_file.exists():
        context.log.warning("No trained piezo model found. Run ml_piezo_model_train first.")
        return

    version = latest_file.read_text().strip()
    path = MODELS_DIR / version
    encoder = SoftCLTEncoder.load(path / "model.pt")
    scaler = joblib.load(path / "scaler.pkl")

    init_ml_schema(pg)

    series_dict, dates_dict = load_piezo_series(pg, min_days=MIN_DAYS)
    context.log.info(f"Encoding {len(series_dict)} piezo stations...")

    station_embs = {}
    window_data = {}
    n_days_map = {}
    n_windows_map = {}

    total = len(series_dict)
    for i, (bss, arr) in enumerate(series_dict.items(), 1):
        scaled = scaler.transform(arr).astype(np.float32)
        dates = dates_dict.get(bss, [])
        n_days_map[bss] = len(arr)

        if len(scaled) < WINDOW_SIZE:
            continue

        win_embs, win_dates = encoder.encode_windows(scaled, WINDOW_SIZE, STRIDE, dates)
        window_data[bss] = (win_embs, win_dates)
        station_embs[bss] = SoftCLTEncoder.station_embedding(win_embs)
        n_windows_map[bss] = win_embs.shape[0]

        if i % 500 == 0 or i == total:
            context.log.info(f"Piezo encoding: {i}/{total} stations ({len(station_embs)} encoded, {sum(w[0].shape[0] for w in window_data.values())} windows)")

    upsert_station_embeddings(pg, "piezo", "code_bss", station_embs, n_days_map, n_windows_map, version)
    upsert_window_embeddings(pg, "piezo", "code_bss", window_data, version)

    total_windows = sum(w[0].shape[0] for w in window_data.values())
    context.add_output_metadata({
        "n_stations": MetadataValue.int(len(station_embs)),
        "n_windows": MetadataValue.int(total_windows),
        "model_version": version,
    })


@asset(
    group_name="ml_hydro",
    deps=["hydro_daily_chroniques"],
    description="Nightly: encode hydro stations → windows + station embeddings",
)
def ml_hydro_embeddings_update(context: AssetExecutionContext, pg: PostgreSQLResource):
    from ..ml.latent_space.encoder import SoftCLTEncoder
    from ..ml.latent_space.data import load_hydro_series
    from ..ml.latent_space.persistence import init_ml_schema, upsert_station_embeddings, upsert_window_embeddings

    latest_file = MODELS_DIR / "hydro_latest"
    if not latest_file.exists():
        context.log.warning("No trained hydro model found. Run ml_hydro_model_train first.")
        return

    version = latest_file.read_text().strip()
    path = MODELS_DIR / version
    encoder = SoftCLTEncoder.load(path / "model.pt")
    scaler = joblib.load(path / "scaler.pkl")

    init_ml_schema(pg)

    series_dict, dates_dict = load_hydro_series(pg, min_days=MIN_DAYS)
    context.log.info(f"Encoding {len(series_dict)} hydro stations...")

    station_embs = {}
    window_data = {}
    n_days_map = {}
    n_windows_map = {}

    total = len(series_dict)
    for i, (station, arr) in enumerate(series_dict.items(), 1):
        scaled = scaler.transform(arr).astype(np.float32)
        dates = dates_dict.get(station, [])
        n_days_map[station] = len(arr)

        if len(scaled) < WINDOW_SIZE:
            continue

        win_embs, win_dates = encoder.encode_windows(scaled, WINDOW_SIZE, STRIDE, dates)
        window_data[station] = (win_embs, win_dates)
        station_embs[station] = SoftCLTEncoder.station_embedding(win_embs)
        n_windows_map[station] = win_embs.shape[0]

        if i % 500 == 0 or i == total:
            context.log.info(f"Hydro encoding: {i}/{total} stations ({len(station_embs)} encoded, {sum(w[0].shape[0] for w in window_data.values())} windows)")

    upsert_station_embeddings(pg, "hydro", "code_station", station_embs, n_days_map, n_windows_map, version)
    upsert_window_embeddings(pg, "hydro", "code_station", window_data, version)

    total_windows = sum(w[0].shape[0] for w in window_data.values())
    context.add_output_metadata({
        "n_stations": MetadataValue.int(len(station_embs)),
        "n_windows": MetadataValue.int(total_windows),
        "model_version": version,
    })


# ======================================================================
# CLUSTERING — After encode, CPU (~30s)
# ======================================================================

def _cluster_and_viz(context, pg, domain: str, id_col: str):
    """Compute 2 clustering configs per domain and store in versioned tables."""
    from ..ml.latent_space.clustering import cluster_and_store
    from ..ml.latent_space.persistence import update_umap_coords, init_ml_schema

    # Ensure new tables exist
    init_ml_schema(pg)

    # --- Config 1: Wide clusters (default) — fewer, larger clusters ---
    context.log.info(f"Config 1: Wide clusters (default) for {domain}...")
    wide = cluster_and_store(
        pg, domain, id_col,
        is_default=True,
        tune=False,
        min_cluster_size=25, min_samples=10,
        umap_dims=15, umap_n_neighbors=20, umap_min_dist=0.1,
    )
    context.log.info(
        f"Wide: {wide['n_clusters']} clusters, DBCV={wide['dbcv']:.4f}, "
        f"sil={wide['silhouette_score']:.4f}, run_id={wide['run_id']}"
    )

    # Update legacy UMAP coords from default run
    if wide.get("umap_2d") is not None:
        update_umap_coords(
            pg, domain, id_col,
            wide["station_ids"], wide["umap_2d"], wide["umap_3d"],
        )

    # --- Config 2: Fine-grained clusters (alternative) ---
    context.log.info(f"Config 2: Fine-grained clusters for {domain}...")
    fine = cluster_and_store(
        pg, domain, id_col,
        is_default=False,
        tune=False,
        min_cluster_size=10, min_samples=5,
        umap_dims=10, umap_n_neighbors=15, umap_min_dist=0.0,
    )
    context.log.info(
        f"Fine: {fine['n_clusters']} clusters, DBCV={fine['dbcv']:.4f}, "
        f"sil={fine['silhouette_score']:.4f}, run_id={fine['run_id']}"
    )

    # Metadata from default (wide) run
    params = wide["params"]
    context.add_output_metadata({
        "n_stations": MetadataValue.int(len(wide["station_ids"])),
        "wide_run_id": MetadataValue.int(wide["run_id"]),
        "wide_n_clusters": MetadataValue.int(wide["n_clusters"]),
        "wide_silhouette": MetadataValue.float(wide["silhouette_score"]),
        "fine_run_id": MetadataValue.int(fine["run_id"]),
        "fine_n_clusters": MetadataValue.int(fine["n_clusters"]),
        "fine_silhouette": MetadataValue.float(fine["silhouette_score"]),
        "hdbscan_min_cluster_size": MetadataValue.int(params["hdbscan_min_cluster_size"]),
        "hdbscan_min_samples": MetadataValue.int(params["hdbscan_min_samples"]),
    })


@asset(
    group_name="ml_piezo",
    deps=["ml_piezo_embeddings_update"],
    description="Optuna-tuned HDBSCAN clustering on piezo station embeddings (~2,935 stations)",
)
def ml_piezo_clusters(context: AssetExecutionContext, pg: PostgreSQLResource):
    _cluster_and_viz(context, pg, "piezo", "code_bss")


@asset(
    group_name="ml_hydro",
    deps=["ml_hydro_embeddings_update"],
    description="Optuna-tuned HDBSCAN clustering on hydro station embeddings (~2,535 stations)",
)
def ml_hydro_clusters(context: AssetExecutionContext, pg: PostgreSQLResource):
    _cluster_and_viz(context, pg, "hydro", "code_station")
