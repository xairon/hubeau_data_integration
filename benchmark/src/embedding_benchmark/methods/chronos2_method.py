"""
Méthode 3 : Chronos-2 — Foundation model zero-shot, multivariate natif (Amazon, Oct 2025).

Encoder-only, 120M params. pipeline.embed() extrait les embeddings du dernier layer.
Supporte nativement plusieurs canaux.
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
    """Encode avec Chronos-2 (zero-shot, multivariate natif)."""
    from chronos import Chronos2Pipeline

    t0 = time.time()
    station_ids = sorted(series.keys())

    device = "cuda" if torch.cuda.is_available() else "cpu"
    pipeline = Chronos2Pipeline.from_pretrained("amazon/chronos-2", device_map=device)

    all_window_embs: Dict[str, np.ndarray] = {}

    for bss in station_ids:
        arr = series[bss]
        T = len(arr)
        if T < cfg.window_size:
            continue

        bss_embs = []
        for w_start in range(0, T - cfg.window_size + 1, cfg.stride):
            window = arr[w_start:w_start + cfg.window_size]

            # Chronos-2 multivariate : tenseur 2D (n_channels, T)
            mv_tensor = torch.tensor(window.T, dtype=torch.float32)  # (4, 365)
            with torch.no_grad():
                emb, _ = pipeline.embed([mv_tensor])
            # emb: (1, n_patches, d_model) → mean pool sur patches
            emb_pooled = emb.squeeze(0).mean(dim=0).cpu().numpy()
            bss_embs.append(emb_pooled)

        all_window_embs[bss] = np.stack(bss_embs)

    # Station = mean pooling
    raw_station = []
    valid_ids = []
    for bss in station_ids:
        if bss in all_window_embs:
            raw_station.append(all_window_embs[bss].mean(axis=0))
            valid_ids.append(bss)

    raw_station = np.stack(raw_station)

    # PCA
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
        method_name="Chronos-2",
    )
