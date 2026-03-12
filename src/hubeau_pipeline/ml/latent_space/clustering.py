"""HDBSCAN clustering on SoftCLT station embeddings with UMAP pre-reduction."""

import numpy as np
import logging

logger = logging.getLogger(__name__)


def cluster_and_update(
    pg,
    domain: str,
    id_col: str,
    min_cluster_size: int = 10,
    umap_dims: int = 10,
) -> dict:
    """Load station embeddings, reduce with UMAP, run HDBSCAN, write cluster_id back to DB.

    UMAP pre-reduction from 320d to umap_dims is critical — HDBSCAN density
    estimation degrades in high-dimensional spaces (curse of dimensionality).

    Returns dict with n_clusters, n_noise, silhouette_score, davies_bouldin_index.
    """
    import hdbscan
    import umap
    from sklearn.metrics import silhouette_score, davies_bouldin_score

    table = f"ml.{domain}_station_embeddings"

    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT {id_col}, embedding::text FROM {table}")
        rows = cur.fetchall()

    if not rows:
        return {"n_clusters": 0, "n_noise": 0, "silhouette_score": -1, "davies_bouldin_index": -1}

    ids = [r[0] for r in rows]
    embs = np.array([[float(x) for x in r[1].strip("[]").split(",")] for r in rows], dtype=np.float32)

    # UMAP dimensionality reduction (320d → umap_dims)
    logger.info(f"UMAP reducing {embs.shape[1]}d → {umap_dims}d for {len(embs)} stations")
    reducer = umap.UMAP(n_components=umap_dims, metric="cosine", random_state=42)
    reduced = reducer.fit_transform(embs)

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=3,
        metric="euclidean",
    )
    labels = clusterer.fit_predict(reduced)

    mask = labels >= 0
    n_clusters = len(set(labels[mask])) if mask.any() else 0

    # Metrics computed on UMAP-reduced space (same space as clustering)
    sil = float(silhouette_score(reduced[mask], labels[mask])) if n_clusters >= 2 else -1.0
    db = float(davies_bouldin_score(reduced[mask], labels[mask])) if n_clusters >= 2 else -1.0

    with pg.get_connection() as conn:
        cur = conn.cursor()
        for sid, label in zip(ids, labels):
            cur.execute(
                f"UPDATE {table} SET cluster_id = %s WHERE {id_col} = %s",
                (int(label), sid),
            )
        conn.commit()

    result = {
        "n_clusters": n_clusters,
        "n_noise": int((labels == -1).sum()),
        "n_clustered": int(mask.sum()),
        "silhouette_score": sil,
        "davies_bouldin_index": db,
    }
    logger.info(f"Clustering {domain}: {result}")
    return result
