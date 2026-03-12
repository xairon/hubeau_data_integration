"""Évaluation des embeddings : clustering, métriques, sérialisation."""

import json
import numpy as np
import pandas as pd
from sklearn.cluster import HDBSCAN
from sklearn.metrics import adjusted_rand_score, silhouette_score
from sklearn.neighbors import NearestNeighbors
from scipy.spatial.distance import cosine

from .config import cfg


# ── Clustering ─────────────────────────────────────────────────────────────


def cluster_hdbscan(embeddings: np.ndarray, min_cluster_size: int = 5,
                    min_samples: int | None = None) -> np.ndarray:
    """HDBSCAN clustering. Returns labels (-1 for noise)."""
    hdb = HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples)
    return hdb.fit_predict(embeddings)


# ── Metrics ────────────────────────────────────────────────────────────────


def eval_silhouette(embeddings: np.ndarray, labels: np.ndarray) -> float:
    """Silhouette score on non-noise points. Returns -1 if <2 clusters."""
    mask = labels >= 0
    n_clusters = len(set(labels[mask]))
    if n_clusters < 2 or mask.sum() < n_clusters:
        return -1.0
    return float(silhouette_score(embeddings[mask], labels[mask]))


def eval_ari(labels: np.ndarray, ground_truth: np.ndarray) -> float:
    """ARI between cluster labels and ground truth. Ignores noise (-1) points."""
    mask = labels >= 0
    if mask.sum() < 2:
        return -1.0
    return float(adjusted_rand_score(ground_truth[mask], labels[mask]))


def eval_temporal_stability(window_embeddings: dict[str, np.ndarray]) -> float:
    """Mean cosine similarity between consecutive windows per station."""
    stabilities = []
    for sid, windows in window_embeddings.items():
        if len(windows) < 2:
            continue
        sims = []
        for i in range(len(windows) - 1):
            sim = 1 - cosine(windows[i], windows[i + 1])
            sims.append(sim)
        stabilities.append(np.mean(sims))
    return float(np.mean(stabilities)) if stabilities else 0.0


def eval_knn_coherence(embeddings: np.ndarray, station_ids: list[str],
                       meta_df: pd.DataFrame, attribute: str, k: int = 10) -> float:
    """Fraction of k-nearest neighbors sharing the same attribute value."""
    meta_map = dict(zip(meta_df["station_id"], meta_df[attribute]))
    attrs = [meta_map.get(sid, "unknown") for sid in station_ids]

    k_actual = min(k, len(station_ids) - 1)
    if k_actual < 1:
        return 0.0

    nn = NearestNeighbors(n_neighbors=k_actual + 1, metric="cosine")
    nn.fit(embeddings)
    _, indices = nn.kneighbors(embeddings)

    coherences = []
    for i, neighbors in enumerate(indices):
        neighbor_attrs = [attrs[j] for j in neighbors[1:]]  # skip self
        same = sum(1 for a in neighbor_attrs if a == attrs[i])
        coherences.append(same / len(neighbor_attrs))
    return float(np.mean(coherences))


# ── Full Evaluation ────────────────────────────────────────────────────────


def run_full_evaluation(embeddings: np.ndarray, station_ids: list[str],
                        domains: list[str], meta_df: pd.DataFrame,
                        window_embeddings: dict[str, np.ndarray] | None = None,
                        method_name: str = "unknown",
                        hdbscan_min_cluster_size: int = 5) -> tuple[dict, np.ndarray]:
    """Run all evaluation metrics. Returns (metrics_dict, cluster_labels)."""
    labels = cluster_hdbscan(embeddings, min_cluster_size=hdbscan_min_cluster_size)

    n_clusters = len(set(labels[labels >= 0]))
    n_noise = int((labels == -1).sum())

    metrics = {
        "method": method_name,
        "n_stations": len(station_ids),
        "n_clusters": n_clusters,
        "n_noise": n_noise,
        "noise_pct": round(100 * n_noise / len(station_ids), 1),
        "silhouette": round(eval_silhouette(embeddings, labels), 4),
    }

    # Domain-specific ARI and kNN
    domain_arr = np.array(domains)

    # Piezo: ARI vs nature_eh
    piezo_mask = domain_arr == "piezo"
    if piezo_mask.sum() > 0 and "nature_eh" in meta_df.columns:
        piezo_gt = meta_df.loc[meta_df["domain"] == "piezo", "nature_eh"].values
        if len(piezo_gt) == piezo_mask.sum():
            metrics["ari_nature_eh"] = round(eval_ari(labels[piezo_mask], piezo_gt), 4)
            metrics["knn_nature_eh"] = round(
                eval_knn_coherence(embeddings[piezo_mask],
                                   [s for s, d in zip(station_ids, domains) if d == "piezo"],
                                   meta_df[meta_df["domain"] == "piezo"], "nature_eh"), 4)

    # Hydro: ARI vs type_site
    hydro_mask = domain_arr == "hydro"
    if hydro_mask.sum() > 0 and "type_site" in meta_df.columns:
        hydro_gt = meta_df.loc[meta_df["domain"] == "hydro", "type_site"].values
        if len(hydro_gt) == hydro_mask.sum():
            metrics["ari_type_site"] = round(eval_ari(labels[hydro_mask], hydro_gt), 4)
            metrics["knn_type_site"] = round(
                eval_knn_coherence(embeddings[hydro_mask],
                                   [s for s, d in zip(station_ids, domains) if d == "hydro"],
                                   meta_df[meta_df["domain"] == "hydro"], "type_site"), 4)

    # Cross-domain kNN coherence (do neighbors share the same domain?)
    metrics["knn_domain"] = round(
        eval_knn_coherence(embeddings, station_ids, meta_df, "domain"), 4)

    # Temporal stability
    if window_embeddings:
        metrics["temporal_stability"] = round(eval_temporal_stability(window_embeddings), 4)

    # Save metrics JSON
    metrics_path = cfg.metrics_dir / f"{method_name}.json"
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    return metrics, labels


# ── Serialization ──────────────────────────────────────────────────────────


def save_embeddings(embeddings: np.ndarray, station_ids: list[str],
                    domains: list[str], labels: np.ndarray,
                    method_name: str, meta_df: pd.DataFrame):
    """Save station embeddings + metadata to Parquet."""
    emb_cols = [f"emb_{i}" for i in range(embeddings.shape[1])]
    df = pd.DataFrame(embeddings, columns=emb_cols)
    df["station_id"] = station_ids
    df["domain"] = domains
    df["cluster_id"] = labels

    # Merge metadata
    meta_cols = [c for c in meta_df.columns if c not in emb_cols]
    df = df.merge(meta_df[meta_cols], on=["station_id", "domain"], how="left")

    path = cfg.embeddings_dir / f"{method_name}.parquet"
    df.to_parquet(path, index=False)
    print(f"  Embeddings saved: {path} ({len(df)} stations)")


def save_window_embeddings(window_embeddings: dict[str, np.ndarray],
                           domains_map: dict[str, str],
                           method_name: str):
    """Save window-level embeddings to Parquet."""
    rows = []
    for sid, windows in window_embeddings.items():
        for widx, emb in enumerate(windows):
            row = {"station_id": sid, "domain": domains_map.get(sid, "unknown"),
                   "window_idx": widx}
            for i, val in enumerate(emb):
                row[f"emb_{i}"] = val
            rows.append(row)

    df = pd.DataFrame(rows)
    path = cfg.windows_dir / f"{method_name}_windows.parquet"
    df.to_parquet(path, index=False)
    print(f"  Window embeddings saved: {path} ({len(df)} windows)")
