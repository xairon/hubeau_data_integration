"""
Dagster Assets — SoftCLT Embeddings (ML Layer)

12 assets in 2 groups (piezo + hydro) x 2 spaces (uni + multi):
- 4 training assets (manual, GPU ~15-30min)
- 4 encoding assets (nightly sensor-driven, GPU ~2-5min)
- 4 clustering assets (after encoding, CPU ~30s)
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
DEPTH = 10
EARLY_STOP_PATIENCE = 20  # Stop if no improvement for 20 epochs

EMBEDDING_DIM = 320  # keep 320 for both spaces (pgvector schema constraint)

# Space-specific hidden_dim: controls capacity of internal representation
# Multi: 4 input dims → 64 hidden (TS2Vec paper default, 16x expansion)
# Uni: 1 input dim → 32 hidden (adapted, avoids 64x overparameterization)
SPACE_HPARAMS = {
    "multi": {"hidden_dim": 64},
    "uni":   {"hidden_dim": 32},
}


# ======================================================================
# HELPERS
# ======================================================================

def _train_encoder(context, pg, domain: str, space: str):
    """Train a SoftCLT encoder for (domain, space)."""
    from ..ml.latent_space.encoder import SoftCLTEncoder
    from ..ml.latent_space.data import (
        load_piezo_series, load_hydro_series,
        load_piezo_series_univariate, load_hydro_series_univariate,
    )

    loaders = {
        ("piezo", "multi"): load_piezo_series,
        ("piezo", "uni"): load_piezo_series_univariate,
        ("hydro", "multi"): load_hydro_series,
        ("hydro", "uni"): load_hydro_series_univariate,
    }
    series_dict, _ = loaders[(domain, space)](pg, min_days=MIN_DAYS)
    context.log.info(f"{len(series_dict)} eligible {domain} stations for {space} encoder")

    if torch.cuda.is_available():
        gpu = torch.cuda.get_device_properties(0)
        context.log.info(f"GPU: {gpu.name}, VRAM: {gpu.total_memory / 1e9:.1f} GB")

    input_dims = 1 if space == "uni" else 4
    hidden_dim = SPACE_HPARAMS[space]["hidden_dim"]
    context.log.info(f"Encoder config: input_dims={input_dims}, hidden_dim={hidden_dim}, embedding_dim={EMBEDDING_DIM}")

    all_data = np.concatenate(list(series_dict.values()))
    scaler = StandardScaler().fit(all_data)
    scaled = [scaler.transform(arr).astype(np.float32) for arr in series_dict.values()]

    t0 = time.time()
    encoder = SoftCLTEncoder(input_dims=input_dims, embedding_dim=EMBEDDING_DIM, hidden_dim=hidden_dim, depth=DEPTH)
    encoder.fit(scaled, n_epochs=N_EPOCHS, lr=1e-3, batch_size=BATCH_SIZE,
                early_stop_patience=EARLY_STOP_PATIENCE, dagster_context=context)
    train_duration = time.time() - t0

    version = f"{domain}_{space}_{datetime.now():%Y%m%d_%H%M}"
    path = MODELS_DIR / version
    path.mkdir(parents=True, exist_ok=True)
    encoder.save(path / "model.pt")
    joblib.dump(scaler, path / "scaler.pkl")
    json.dump(list(series_dict.keys()), (path / "stations.json").open("w"))
    (MODELS_DIR / f"{domain}_{space}_latest").write_text(version)

    context.add_output_metadata({
        "model_version": version,
        "space": space,
        "input_dims": MetadataValue.int(input_dims),
        "n_stations": MetadataValue.int(len(series_dict)),
        "train_duration_sec": MetadataValue.float(train_duration),
    })


def _encode_stations(context, pg, domain: str, id_col: str, space: str):
    """Nightly: encode stations with a trained (domain, space) model."""
    from ..ml.latent_space.encoder import SoftCLTEncoder
    from ..ml.latent_space.data import (
        load_piezo_series, load_hydro_series,
        load_piezo_series_univariate, load_hydro_series_univariate,
    )
    from ..ml.latent_space.persistence import init_ml_schema, upsert_station_embeddings, upsert_window_embeddings

    latest_file = MODELS_DIR / f"{domain}_{space}_latest"
    if not latest_file.exists() and space == "multi":
        latest_file = MODELS_DIR / f"{domain}_latest"  # backward compat
    if not latest_file.exists():
        context.log.warning(f"No trained {domain}/{space} model. Run ml_{domain}_{space}_model_train first.")
        return

    version = latest_file.read_text().strip()
    path = MODELS_DIR / version
    encoder = SoftCLTEncoder.load(path / "model.pt")
    scaler = joblib.load(path / "scaler.pkl")

    init_ml_schema(pg)

    loaders = {
        ("piezo", "multi"): load_piezo_series,
        ("piezo", "uni"): load_piezo_series_univariate,
        ("hydro", "multi"): load_hydro_series,
        ("hydro", "uni"): load_hydro_series_univariate,
    }
    series_dict, dates_dict = loaders[(domain, space)](pg, min_days=MIN_DAYS)
    context.log.info(f"Encoding {len(series_dict)} {domain}/{space} stations...")

    station_embs = {}
    window_data = {}
    n_days_map = {}
    n_windows_map = {}

    total = len(series_dict)
    for i, (sid, arr) in enumerate(series_dict.items(), 1):
        scaled = scaler.transform(arr).astype(np.float32)
        dates = dates_dict.get(sid, [])
        n_days_map[sid] = len(arr)
        if len(scaled) < WINDOW_SIZE:
            continue
        win_embs, win_dates = encoder.encode_windows(scaled, WINDOW_SIZE, STRIDE, dates)
        window_data[sid] = (win_embs, win_dates)
        station_embs[sid] = SoftCLTEncoder.station_embedding(win_embs)
        n_windows_map[sid] = win_embs.shape[0]
        if i % 500 == 0 or i == total:
            context.log.info(f"{domain}/{space}: {i}/{total} stations")

    upsert_station_embeddings(pg, domain, id_col, station_embs, n_days_map, n_windows_map, version, space=space)
    upsert_window_embeddings(pg, domain, id_col, window_data, version, space=space)

    context.add_output_metadata({
        "n_stations": MetadataValue.int(len(station_embs)),
        "n_windows": MetadataValue.int(sum(w[0].shape[0] for w in window_data.values())),
        "model_version": version,
        "space": space,
    })


def _cluster_and_viz(context, pg, domain: str, id_col: str, space: str):
    """Compute 2 clustering configs per (domain, space) and store in versioned tables."""
    from ..ml.latent_space.clustering import cluster_and_store
    from ..ml.latent_space.persistence import update_umap_coords, init_ml_schema

    # Ensure new tables exist
    init_ml_schema(pg)

    # Space-specific UMAP pre-reduction params
    # Uni embeddings (128d) have lower intrinsic dimensionality → smaller UMAP target
    # Multi embeddings (320d) need more room
    if space == "uni":
        wide_umap = {"umap_dims": 5, "umap_n_neighbors": 10, "umap_min_dist": 0.1}
        fine_umap = {"umap_dims": 5, "umap_n_neighbors": 8, "umap_min_dist": 0.0}
    else:
        wide_umap = {"umap_dims": 15, "umap_n_neighbors": 20, "umap_min_dist": 0.1}
        fine_umap = {"umap_dims": 10, "umap_n_neighbors": 15, "umap_min_dist": 0.0}

    # --- Config 1: Wide clusters (default) — fewer, larger clusters ---
    context.log.info(f"Config 1: Wide clusters (default) for {domain}/{space} — UMAP {wide_umap}")
    wide = cluster_and_store(
        pg, domain, id_col,
        is_default=True,
        tune=False,
        min_cluster_size=25, min_samples=10,
        **wide_umap,
        space=space,
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
    context.log.info(f"Config 2: Fine-grained clusters for {domain}/{space} — UMAP {fine_umap}")
    fine = cluster_and_store(
        pg, domain, id_col,
        is_default=False,
        tune=False,
        min_cluster_size=10, min_samples=5,
        **fine_umap,
        space=space,
    )
    context.log.info(
        f"Fine: {fine['n_clusters']} clusters, DBCV={fine['dbcv']:.4f}, "
        f"sil={fine['silhouette_score']:.4f}, run_id={fine['run_id']}"
    )

    # Metadata from default (wide) run
    params = wide["params"]
    context.add_output_metadata({
        "n_stations": MetadataValue.int(len(wide["station_ids"])),
        "space": space,
        "wide_run_id": MetadataValue.int(wide["run_id"]),
        "wide_n_clusters": MetadataValue.int(wide["n_clusters"]),
        "wide_silhouette": MetadataValue.float(wide["silhouette_score"]),
        "fine_run_id": MetadataValue.int(fine["run_id"]),
        "fine_n_clusters": MetadataValue.int(fine["n_clusters"]),
        "fine_silhouette": MetadataValue.float(fine["silhouette_score"]),
        "hdbscan_min_cluster_size": MetadataValue.int(params["hdbscan_min_cluster_size"]),
        "hdbscan_min_samples": MetadataValue.int(params["hdbscan_min_samples"]),
    })


# ======================================================================
# TRAINING — Manual, GPU (~15-30min per domain per space)
# ======================================================================

@asset(group_name="ml_piezo", deps=["hubeau_daily_chroniques"],
       description="Train SoftCLT encoder for piezo MULTIVARIATE (4 vars)")
def ml_piezo_multi_model_train(context: AssetExecutionContext, pg: PostgreSQLResource):
    _train_encoder(context, pg, "piezo", "multi")


@asset(group_name="ml_piezo", deps=["hubeau_daily_chroniques"],
       description="Train SoftCLT encoder for piezo UNIVARIATE (target only)")
def ml_piezo_uni_model_train(context: AssetExecutionContext, pg: PostgreSQLResource):
    _train_encoder(context, pg, "piezo", "uni")


@asset(group_name="ml_hydro", deps=["hydro_daily_chroniques"],
       description="Train SoftCLT encoder for hydro MULTIVARIATE (4 vars)")
def ml_hydro_multi_model_train(context: AssetExecutionContext, pg: PostgreSQLResource):
    _train_encoder(context, pg, "hydro", "multi")


@asset(group_name="ml_hydro", deps=["hydro_daily_chroniques"],
       description="Train SoftCLT encoder for hydro UNIVARIATE (target only)")
def ml_hydro_uni_model_train(context: AssetExecutionContext, pg: PostgreSQLResource):
    _train_encoder(context, pg, "hydro", "uni")


# ======================================================================
# NIGHTLY ENCODE — Sensor-driven, GPU (~2-5min per domain per space)
# ======================================================================

@asset(group_name="ml_piezo", deps=["hubeau_daily_chroniques"],
       description="Nightly: encode piezo MULTI embeddings")
def ml_piezo_multi_embeddings_update(context: AssetExecutionContext, pg: PostgreSQLResource):
    _encode_stations(context, pg, "piezo", "code_bss", "multi")


@asset(group_name="ml_piezo", deps=["hubeau_daily_chroniques"],
       description="Nightly: encode piezo UNI embeddings")
def ml_piezo_uni_embeddings_update(context: AssetExecutionContext, pg: PostgreSQLResource):
    _encode_stations(context, pg, "piezo", "code_bss", "uni")


@asset(group_name="ml_hydro", deps=["hydro_daily_chroniques"],
       description="Nightly: encode hydro MULTI embeddings")
def ml_hydro_multi_embeddings_update(context: AssetExecutionContext, pg: PostgreSQLResource):
    _encode_stations(context, pg, "hydro", "code_station", "multi")


@asset(group_name="ml_hydro", deps=["hydro_daily_chroniques"],
       description="Nightly: encode hydro UNI embeddings")
def ml_hydro_uni_embeddings_update(context: AssetExecutionContext, pg: PostgreSQLResource):
    _encode_stations(context, pg, "hydro", "code_station", "uni")


# ======================================================================
# CLUSTERING — After encode, CPU (~30s per domain per space)
# ======================================================================

@asset(group_name="ml_piezo", deps=["ml_piezo_multi_embeddings_update"],
       description="Cluster piezo MULTI embeddings")
def ml_piezo_multi_clusters(context: AssetExecutionContext, pg: PostgreSQLResource):
    _cluster_and_viz(context, pg, "piezo", "code_bss", "multi")


@asset(group_name="ml_piezo", deps=["ml_piezo_uni_embeddings_update"],
       description="Cluster piezo UNI embeddings")
def ml_piezo_uni_clusters(context: AssetExecutionContext, pg: PostgreSQLResource):
    _cluster_and_viz(context, pg, "piezo", "code_bss", "uni")


@asset(group_name="ml_hydro", deps=["ml_hydro_multi_embeddings_update"],
       description="Cluster hydro MULTI embeddings")
def ml_hydro_multi_clusters(context: AssetExecutionContext, pg: PostgreSQLResource):
    _cluster_and_viz(context, pg, "hydro", "code_station", "multi")


@asset(group_name="ml_hydro", deps=["ml_hydro_uni_embeddings_update"],
       description="Cluster hydro UNI embeddings")
def ml_hydro_uni_clusters(context: AssetExecutionContext, pg: PostgreSQLResource):
    _cluster_and_viz(context, pg, "hydro", "code_station", "uni")
