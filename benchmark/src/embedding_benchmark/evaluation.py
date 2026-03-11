"""
Métriques d'évaluation non-supervisées pour comparer les embeddings.

4 métriques :
1. Silhouette score (HDBSCAN clusters)
2. ARI vs nature_eh (cohérence hydrogéologique)
3. Stabilité temporelle (corrélation cosinus fenêtres consécutives)
4. kNN cohérence (voisins proches = même nature_eh)
"""

import json
import numpy as np
import pandas as pd
import hdbscan
from pathlib import Path
from sklearn.metrics import silhouette_score, adjusted_rand_score
from sklearn.neighbors import NearestNeighbors
from typing import Dict

from .config import cfg


def cluster_hdbscan(embeddings: np.ndarray, min_cluster_size: int = 5) -> np.ndarray:
    """HDBSCAN clustering. Retourne labels (noise = -1)."""
    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=3, metric="euclidean")
    return clusterer.fit_predict(embeddings)


def eval_silhouette(embeddings: np.ndarray, labels: np.ndarray) -> float:
    """Silhouette score sur les points non-noise. [-1, 1], plus haut = mieux."""
    mask = labels >= 0
    if mask.sum() < 2 or len(set(labels[mask])) < 2:
        return -1.0
    return float(silhouette_score(embeddings[mask], labels[mask]))


def eval_ari_nature_eh(labels: np.ndarray, nature_eh: list[str]) -> float:
    """ARI entre clusters HDBSCAN et nature_eh. [-1, 1], 0 = random, 1 = parfait."""
    mask = np.array(labels) >= 0
    if mask.sum() < 2:
        return -1.0
    return float(adjusted_rand_score(np.array(nature_eh)[mask], labels[mask]))


def eval_temporal_stability(window_embeddings: Dict[str, np.ndarray]) -> float:
    """Corrélation cosinus moyenne entre fenêtres consécutives. [0, 1], haut = stable."""
    correlations = []
    for embs in window_embeddings.values():
        if len(embs) < 2:
            continue
        for i in range(len(embs) - 1):
            a, b = embs[i], embs[i + 1]
            norm_a, norm_b = np.linalg.norm(a), np.linalg.norm(b)
            if norm_a == 0 or norm_b == 0:
                continue
            correlations.append(np.dot(a, b) / (norm_a * norm_b))
    return float(np.mean(correlations)) if correlations else -1.0


def eval_knn_coherence(
    embeddings: np.ndarray,
    station_ids: list[str],
    station_meta: pd.DataFrame,
    k: int = 10,
    col: str = "nature_eh",
) -> float:
    """% de kNN qui partagent le même attribut. [0, 1], haut = cohérent."""
    nn = NearestNeighbors(n_neighbors=k + 1, metric="cosine")
    nn.fit(embeddings)
    _, indices = nn.kneighbors(embeddings)

    meta_map = dict(zip(station_meta["code_bss"], station_meta[col]))
    values = [meta_map.get(sid, "UNKNOWN") for sid in station_ids]

    coherences = []
    for i, neighbors in enumerate(indices):
        if values[i] == "UNKNOWN":
            continue
        neighbor_vals = [values[j] for j in neighbors[1:] if j < len(values)]
        if not neighbor_vals:
            continue
        coherences.append(sum(1 for v in neighbor_vals if v == values[i]) / len(neighbor_vals))

    return float(np.mean(coherences)) if coherences else -1.0


def run_full_evaluation(
    station_embeddings: np.ndarray,
    station_ids: list[str],
    station_meta: pd.DataFrame,
    window_embeddings: Dict[str, np.ndarray] | None = None,
    method_name: str = "unknown",
) -> Dict:
    """Lance les 4 métriques, retourne un dict, sauvegarde en JSON."""
    labels = cluster_hdbscan(station_embeddings)
    n_clusters = len(set(labels[labels >= 0]))
    n_noise = int((labels == -1).sum())

    nature_eh_list = []
    for sid in station_ids:
        match = station_meta.loc[station_meta["code_bss"] == sid, "nature_eh"]
        nature_eh_list.append(match.iloc[0] if len(match) > 0 else "UNKNOWN")

    result = {
        "method": method_name,
        "n_stations": len(station_ids),
        "n_clusters": n_clusters,
        "n_noise": n_noise,
        "noise_pct": round(n_noise / len(station_ids) * 100, 1),
        "silhouette": round(eval_silhouette(station_embeddings, labels), 4),
        "ari_nature_eh": round(eval_ari_nature_eh(labels, nature_eh_list), 4),
        "temporal_stability": round(
            eval_temporal_stability(window_embeddings) if window_embeddings else -1.0, 4
        ),
        "knn_coherence_nature_eh": round(
            eval_knn_coherence(station_embeddings, station_ids, station_meta), 4
        ),
    }

    # Sauvegarder les métriques en JSON
    out = cfg.metrics_dir / f"{method_name}.json"
    with open(out, "w") as f:
        json.dump(result, f, indent=2)

    return result


def save_embeddings(
    station_embeddings: np.ndarray,
    station_ids: list[str],
    labels: np.ndarray,
    method_name: str,
    station_meta: pd.DataFrame,
) -> Path:
    """Sauvegarde les embeddings station en Parquet (pour l'UI Streamlit)."""
    df = pd.DataFrame(station_embeddings, columns=[f"emb_{i}" for i in range(station_embeddings.shape[1])])
    df.insert(0, "code_bss", station_ids)
    df["cluster_id"] = labels

    # Joindre les métadonnées
    meta_cols = ["code_bss", "nature_eh", "milieu_eh", "n_days", "first_date", "last_date"]
    available = [c for c in meta_cols if c in station_meta.columns]
    df = df.merge(station_meta[available], on="code_bss", how="left")

    out = cfg.embeddings_dir / f"{method_name}.parquet"
    df.to_parquet(out, index=False)
    return out
