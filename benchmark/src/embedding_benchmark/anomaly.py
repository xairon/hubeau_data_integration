"""Détection d'anomalies dans l'espace latent."""

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor, NearestNeighbors


def detect_anomalies_iforest(embeddings: np.ndarray, contamination: float = 0.05,
                             random_state: int = 42) -> np.ndarray:
    """Isolation Forest anomaly detection. Returns scores (lower = more anomalous)."""
    clf = IsolationForest(contamination=contamination, random_state=random_state)
    clf.fit(embeddings)
    return clf.decision_function(embeddings)


def detect_anomalies_lof(embeddings: np.ndarray, contamination: float = 0.05,
                         n_neighbors: int = 20) -> np.ndarray:
    """Local Outlier Factor. Returns scores (lower = more anomalous)."""
    n_neighbors = min(n_neighbors, len(embeddings) - 1)
    clf = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=contamination)
    clf.fit_predict(embeddings)
    return clf.negative_outlier_factor_


def find_nearest_normal(embeddings: np.ndarray, anomaly_mask: np.ndarray) -> np.ndarray:
    """For each anomalous point, find nearest non-anomalous point. Returns indices."""
    normal_embeddings = embeddings[~anomaly_mask]
    normal_indices = np.where(~anomaly_mask)[0]

    if len(normal_embeddings) == 0:
        return np.full(anomaly_mask.sum(), -1, dtype=int)

    nn = NearestNeighbors(n_neighbors=1, metric="cosine")
    nn.fit(normal_embeddings)

    anomaly_embeddings = embeddings[anomaly_mask]
    _, indices = nn.kneighbors(anomaly_embeddings)
    return normal_indices[indices.ravel()]


def build_anomaly_table(embeddings: np.ndarray, station_ids: list[str],
                        domains: list[str], scores: np.ndarray,
                        contamination: float = 0.05) -> pd.DataFrame:
    """Build a DataFrame of anomalies with nearest normal neighbor."""
    threshold = np.percentile(scores, contamination * 100)
    anomaly_mask = scores <= threshold

    nearest_normal = find_nearest_normal(embeddings, anomaly_mask)

    rows = []
    anom_idx = 0
    for i in range(len(station_ids)):
        if anomaly_mask[i]:
            nn_idx = nearest_normal[anom_idx]
            rows.append({
                "station_id": station_ids[i],
                "domain": domains[i],
                "anomaly_score": round(float(scores[i]), 4),
                "nearest_normal_id": station_ids[nn_idx] if nn_idx >= 0 else None,
                "nearest_normal_domain": domains[nn_idx] if nn_idx >= 0 else None,
            })
            anom_idx += 1

    return pd.DataFrame(rows).sort_values("anomaly_score")
