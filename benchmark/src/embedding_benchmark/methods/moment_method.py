"""
Méthode 2 : MOMENT — Foundation model zero-shot (ICML 2024).

MOMENT est univariate : encode chaque canal séparément puis concatène.
Dimension brute = 4 × d_model (4 × 1024 = 4096 pour MOMENT-1-large).
PCA réduit à embedding_dim.

Limitation documentée : pas de capture des inter-dépendances entre variables.
"""

import time
import numpy as np
import torch
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from typing import Dict

from ..config import cfg
from . import MethodResult


def run(series: Dict[str, np.ndarray], dates: Dict[str, list]) -> MethodResult:
    """Encode avec MOMENT (zero-shot, univariate par canal)."""
    from momentfm import MOMENTPipeline

    t0 = time.time()
    station_ids = sorted(series.keys())
    n_vars = len(cfg.piezo_cols)

    model = MOMENTPipeline.from_pretrained(
        "AutonLab/MOMENT-1-large",
        model_kwargs={"task_name": "embedding"},
    )
    model.init()

    # MOMENT max input = 512 points
    input_len = min(cfg.window_size, 512)

    all_window_embs: Dict[str, np.ndarray] = {}

    for bss in station_ids:
        arr = series[bss]
        T = len(arr)
        if T < cfg.window_size:
            continue

        bss_embs = []
        for w_start in range(0, T - cfg.window_size + 1, cfg.stride):
            window = arr[w_start:w_start + cfg.window_size]

            # Encoder chaque variable séparément
            chan_embs = []
            for v_idx in range(n_vars):
                chan = window[:input_len, v_idx]
                x = torch.tensor(chan, dtype=torch.float32).unsqueeze(0).unsqueeze(0)
                with torch.no_grad():
                    output = model(x)
                emb = output.embeddings.squeeze().cpu().numpy()
                chan_embs.append(emb)

            bss_embs.append(np.concatenate(chan_embs))

        all_window_embs[bss] = np.stack(bss_embs)

    # Station = mean pooling des fenêtres
    raw_station = []
    valid_ids = []
    for bss in station_ids:
        if bss in all_window_embs:
            raw_station.append(all_window_embs[bss].mean(axis=0))
            valid_ids.append(bss)

    raw_station = np.stack(raw_station)

    # PCA vers embedding_dim
    scaler = StandardScaler()
    scaled = scaler.fit_transform(raw_station)
    dim = min(cfg.embedding_dim, scaled.shape[1], scaled.shape[0] - 1)
    pca = PCA(n_components=dim)
    station_emb = pca.fit_transform(scaled).astype(np.float32)

    if station_emb.shape[1] < cfg.embedding_dim:
        pad = np.zeros((station_emb.shape[0], cfg.embedding_dim - station_emb.shape[1]), dtype=np.float32)
        station_emb = np.concatenate([station_emb, pad], axis=1)

    window_embeddings = {}
    for bss in valid_ids:
        raw = all_window_embs[bss]
        win_scaled = scaler.transform(raw)
        win_pca = pca.transform(win_scaled).astype(np.float32)
        if win_pca.shape[1] < cfg.embedding_dim:
            pad = np.zeros((win_pca.shape[0], cfg.embedding_dim - win_pca.shape[1]), dtype=np.float32)
            win_pca = np.concatenate([win_pca, pad], axis=1)
        window_embeddings[bss] = win_pca

    elapsed = time.time() - t0
    return MethodResult(
        station_embeddings=station_emb,
        station_ids=valid_ids,
        window_embeddings=window_embeddings,
        elapsed_seconds=elapsed,
        method_name="MOMENT",
    )
