"""
Méthode 1 : tsfresh — extraction de features statistiques + PCA.

Baseline interprétable. Pas d'apprentissage, pas de GPU.
Utilise EfficientFCParameters pour limiter le nombre de features (~200 au lieu de ~800).
"""

import time
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from tsfresh import extract_features
from tsfresh.feature_extraction import EfficientFCParameters
from tsfresh.utilities.dataframe_functions import impute
from typing import Dict

from ..config import cfg
from . import MethodResult


def _series_to_tsfresh_df(series: Dict[str, np.ndarray], var_names: list[str]) -> pd.DataFrame:
    """Convertit {id: (T, 4)} en DataFrame long pour tsfresh."""
    rows = []
    for bss, arr in series.items():
        for t_idx in range(len(arr)):
            row = {"id": bss, "time": t_idx}
            for v_idx, vname in enumerate(var_names):
                row[vname] = float(arr[t_idx, v_idx])
            rows.append(row)
    return pd.DataFrame(rows)


def _extract_and_reduce(df_long: pd.DataFrame, embedding_dim: int, scaler=None, pca=None, fit: bool = True):
    """Extrait features tsfresh, normalise, PCA."""
    features = extract_features(
        df_long,
        column_id="id",
        column_sort="time",
        default_fc_parameters=EfficientFCParameters(),
        disable_progressbar=False,
        n_jobs=4,
    )
    features = impute(features)

    if fit:
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)
        dim = min(embedding_dim, features_scaled.shape[1], features_scaled.shape[0] - 1)
        pca = PCA(n_components=dim)
        embeddings = pca.fit_transform(features_scaled).astype(np.float32)
    else:
        features_scaled = scaler.transform(features)
        embeddings = pca.transform(features_scaled).astype(np.float32)

    # Pad si nécessaire
    if embeddings.shape[1] < embedding_dim:
        pad = np.zeros((embeddings.shape[0], embedding_dim - embeddings.shape[1]), dtype=np.float32)
        embeddings = np.concatenate([embeddings, pad], axis=1)

    return embeddings, features.index.tolist(), scaler, pca


def run(series: Dict[str, np.ndarray], dates: Dict[str, list]) -> MethodResult:
    """Exécute tsfresh sur les séries et fenêtres."""
    t0 = time.time()

    # Station-level
    df_long = _series_to_tsfresh_df(series, cfg.piezo_cols)
    station_emb, station_ids, scaler, pca = _extract_and_reduce(df_long, cfg.embedding_dim, fit=True)

    # Window-level
    window_embeddings: Dict[str, np.ndarray] = {}
    for bss, arr in series.items():
        T = len(arr)
        if T < cfg.window_size:
            continue
        win_rows = []
        for w_start in range(0, T - cfg.window_size + 1, cfg.stride):
            w_end = w_start + cfg.window_size
            win_id = f"{bss}_w{w_start}"
            for t_idx in range(cfg.window_size):
                row = {"id": win_id, "time": t_idx}
                for v_idx, vname in enumerate(cfg.piezo_cols):
                    row[vname] = float(arr[w_start + t_idx, v_idx])
                win_rows.append(row)

        if not win_rows:
            continue

        df_win = pd.DataFrame(win_rows)
        win_features = extract_features(
            df_win, column_id="id", column_sort="time",
            default_fc_parameters=EfficientFCParameters(),
            disable_progressbar=True, n_jobs=4,
        )
        win_features = impute(win_features)
        win_scaled = scaler.transform(win_features)
        win_emb = pca.transform(win_scaled).astype(np.float32)
        if win_emb.shape[1] < cfg.embedding_dim:
            pad = np.zeros((win_emb.shape[0], cfg.embedding_dim - win_emb.shape[1]), dtype=np.float32)
            win_emb = np.concatenate([win_emb, pad], axis=1)
        window_embeddings[bss] = win_emb

    elapsed = time.time() - t0
    return MethodResult(
        station_embeddings=station_emb,
        station_ids=station_ids,
        window_embeddings=window_embeddings,
        elapsed_seconds=elapsed,
        method_name="tsfresh",
    )
