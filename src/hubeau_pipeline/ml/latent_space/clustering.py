"""HDBSCAN clustering on SoftCLT station embeddings with UMAP pre-reduction.

Supports two modes:
  1. Fixed params (default): uses provided or default hyperparameters.
  2. Tuned params: runs Optuna hyperparameter optimization first, then
     clusters with best params.
"""

import numpy as np
import logging

logger = logging.getLogger(__name__)


def cluster_and_update(
    pg,
    domain: str,
    id_col: str,
    min_cluster_size: int | None = None,
    min_samples: int | None = None,
    umap_dims: int | None = None,
    umap_n_neighbors: int | None = None,
    umap_min_dist: float | None = None,
    tune: bool = False,
    tune_n_trials: int = 80,
    tune_timeout: int = 300,
    space: str = "multi",
) -> dict:
    """Load station embeddings, reduce with UMAP, run HDBSCAN, write cluster_id back to DB.

    When tune=True, runs Optuna optimization to find best params first.
    When tune=False, uses provided params or defaults.

    Returns dict with metrics + embeddings + station_ids (for downstream UMAP viz).
    """
    import hdbscan
    import umap
    from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score

    table = f"ml.{domain}_station_embeddings"

    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT {id_col}, embedding::text FROM {table} WHERE space = %s", (space,))
        rows = cur.fetchall()

    if not rows:
        return {
            "n_clusters": 0, "n_noise": 0,
            "silhouette_score": -1, "davies_bouldin_index": -1,
            "embeddings": np.array([]), "station_ids": [],
        }

    ids = [r[0] for r in rows]
    embs = np.array([[float(x) for x in r[1].strip("[]").split(",")] for r in rows], dtype=np.float32)

    if tune:
        from .tuning import tune_clustering

        logger.info(f"Running hyperparameter tuning for {domain} ({len(embs)} stations, {tune_n_trials} trials)...")
        tuning_result = tune_clustering(
            embs, n_trials=tune_n_trials, timeout=tune_timeout,
        )
        logger.info(f"Tuning result: {tuning_result.to_dict()}")

        # Use tuned params
        _umap_dims = tuning_result.umap_n_components
        _umap_n_neighbors = tuning_result.umap_n_neighbors
        _umap_min_dist = tuning_result.umap_min_dist
        _min_cluster_size = tuning_result.hdbscan_min_cluster_size
        _min_samples = tuning_result.hdbscan_min_samples
    else:
        # Use provided or defaults
        _umap_dims = umap_dims or 10
        _umap_n_neighbors = umap_n_neighbors or 15
        _umap_min_dist = umap_min_dist if umap_min_dist is not None else 0.0
        _min_cluster_size = min_cluster_size or 10
        _min_samples = min_samples or 3

    # UMAP dimensionality reduction
    logger.info(
        f"UMAP reducing {embs.shape[1]}d → {_umap_dims}d for {len(embs)} {domain} stations "
        f"(n_neighbors={_umap_n_neighbors}, min_dist={_umap_min_dist})"
    )
    reducer = umap.UMAP(
        n_components=_umap_dims,
        n_neighbors=_umap_n_neighbors,
        min_dist=_umap_min_dist,
        metric="cosine",
        random_state=42,
    )
    reduced = reducer.fit_transform(embs)

    # HDBSCAN clustering
    logger.info(
        f"HDBSCAN clustering (min_cluster_size={_min_cluster_size}, min_samples={_min_samples})"
    )
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=_min_cluster_size,
        min_samples=_min_samples,
        metric="euclidean",
    )
    labels = clusterer.fit_predict(reduced)

    mask = labels >= 0
    n_clusters = len(set(labels[mask])) if mask.any() else 0
    n_noise = int((labels == -1).sum())

    # Quality metrics
    sil = float(silhouette_score(reduced[mask], labels[mask])) if n_clusters >= 2 else -1.0
    db = float(davies_bouldin_score(reduced[mask], labels[mask])) if n_clusters >= 2 else -1.0
    ch = float(calinski_harabasz_score(reduced[mask], labels[mask])) if n_clusters >= 2 else -1.0
    dbcv = float(getattr(clusterer, "relative_validity_", 0.0))

    # Write cluster labels to DB
    with pg.get_connection() as conn:
        cur = conn.cursor()
        for sid, label in zip(ids, labels):
            cur.execute(
                f"UPDATE {table} SET cluster_id = %s WHERE {id_col} = %s AND space = %s",
                (int(label), sid, space),
            )
        conn.commit()

    result = {
        "n_clusters": n_clusters,
        "n_noise": n_noise,
        "n_clustered": int(mask.sum()),
        "noise_ratio": round(n_noise / len(labels), 4),
        "silhouette_score": sil,
        "davies_bouldin_index": db,
        "calinski_harabasz": ch,
        "dbcv": dbcv,
        # Params used (useful for logging/metadata)
        "params": {
            "umap_n_components": _umap_dims,
            "umap_n_neighbors": _umap_n_neighbors,
            "umap_min_dist": _umap_min_dist,
            "hdbscan_min_cluster_size": _min_cluster_size,
            "hdbscan_min_samples": _min_samples,
            "tuned": tune,
        },
        # For downstream UMAP visualization
        "embeddings": embs,
        "station_ids": ids,
    }
    logger.info(f"Clustering {domain}: {n_clusters} clusters, {n_noise} noise, DBCV={dbcv:.4f}, sil={sil:.4f}")
    return result


def cluster_and_store(
    pg,
    domain: str,
    id_col: str,
    is_default: bool = False,
    tune: bool = False,
    tune_n_trials: int = 80,
    tune_timeout: int = 300,
    min_cluster_size: int | None = None,
    min_samples: int | None = None,
    umap_dims: int | None = None,
    umap_n_neighbors: int | None = None,
    umap_min_dist: float | None = None,
    space: str = "multi",
) -> dict:
    """Cluster station embeddings and store results in ml.clustering_runs/labels.

    Also computes UMAP 2D/3D visualization coords.
    Returns dict with run_id, metrics, params, embeddings, station_ids.
    """
    import umap as umap_lib
    from .persistence import save_clustering_run

    result = cluster_and_update(
        pg, domain, id_col,
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        umap_dims=umap_dims,
        umap_n_neighbors=umap_n_neighbors,
        umap_min_dist=umap_min_dist,
        tune=tune,
        tune_n_trials=tune_n_trials,
        tune_timeout=tune_timeout,
        space=space,
    )

    embeddings = result["embeddings"]
    station_ids = result["station_ids"]

    if len(station_ids) == 0:
        return result

    # Compute UMAP 2D/3D for visualization
    # Uni embeddings have lower intrinsic dim → use fewer neighbors for sharper structure
    viz_nn = 15 if space == "uni" else 30
    viz_md = 0.02 if space == "uni" else 0.05
    logger.info(f"Computing UMAP 2D/3D for {len(station_ids)} {domain}/{space} stations (nn={viz_nn}, md={viz_md})...")
    umap_2d = umap_lib.UMAP(
        n_components=2, n_neighbors=viz_nn, min_dist=viz_md,
        metric="cosine", random_state=42,
    ).fit_transform(embeddings)

    umap_3d = umap_lib.UMAP(
        n_components=3, n_neighbors=viz_nn, min_dist=viz_md,
        metric="cosine", random_state=42,
    ).fit_transform(embeddings)

    # Re-extract labels from DB (cluster_and_update already wrote them)
    table = f"ml.{domain}_station_embeddings"
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT {id_col}, cluster_id FROM {table} WHERE space = %s", (space,))
        db_labels = {r[0]: r[1] for r in cur.fetchall()}
    labels = np.array([db_labels.get(sid, -1) for sid in station_ids])

    metrics = {
        "silhouette": result["silhouette_score"],
        "davies_bouldin": result["davies_bouldin_index"],
        "calinski_harabasz": result["calinski_harabasz"],
        "dbcv": result["dbcv"],
        "noise_ratio": result["noise_ratio"],
    }

    run_id = save_clustering_run(
        pg,
        domain=domain,
        level="stations",
        method="hdbscan",
        params=result["params"],
        metrics=metrics,
        n_clusters=result["n_clusters"],
        n_stations=len(station_ids),
        is_default=is_default,
        station_ids=station_ids,
        labels=labels,
        umap_2d=umap_2d,
        umap_3d=umap_3d,
        space=space,
    )

    result["run_id"] = run_id
    result["umap_2d"] = umap_2d
    result["umap_3d"] = umap_3d
    return result
