"""
Méthode 4 : TS2Vec — Contrastif hiérarchique (AAAI 2022).

Entraîné sur nos données. Multivariate natif.
"""

import time
import torch
import numpy as np
from sklearn.preprocessing import StandardScaler
from typing import Dict

from ..config import cfg
from ..vendors.ts2vec.ts2vec import TS2Vec
from . import MethodResult


def run(
    series: Dict[str, np.ndarray],
    dates: Dict[str, list],
    n_epochs: int = 200,
    lr: float = 1e-3,
    batch_size: int = 16,
    depth: int = 10,
) -> MethodResult:
    """Entraîne TS2Vec puis encode fenêtres + stations."""
    t0 = time.time()
    station_ids = sorted(series.keys())
    n_vars = len(cfg.piezo_cols)

    # Normalisation globale
    all_data = np.concatenate(list(series.values()))
    scaler = StandardScaler().fit(all_data)
    scaled = {bss: scaler.transform(arr) for bss, arr in series.items()}

    # TS2Vec accepte une liste de (T_i, C) avec longueurs variables
    train_data = [scaled[bss].astype(np.float32) for bss in station_ids]

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = TS2Vec(
        input_dims=n_vars,
        output_dims=cfg.embedding_dim,
        hidden_dims=cfg.embedding_dim,
        depth=depth,
        device=device,
    )
    model.fit(train_data, n_epochs=n_epochs, lr=lr, batch_size=batch_size, verbose=True)

    # Encoder fenêtres
    window_embeddings: Dict[str, np.ndarray] = {}
    station_emb_list = []
    valid_ids = []

    for bss in station_ids:
        arr = scaled[bss].astype(np.float32)
        T = len(arr)
        if T < cfg.window_size:
            continue

        bss_embs = []
        for w_start in range(0, T - cfg.window_size + 1, cfg.stride):
            window = arr[w_start:w_start + cfg.window_size]
            emb = model.encode(window[np.newaxis], encoding_window="full_series")
            bss_embs.append(emb.squeeze())

        win_embs = np.stack(bss_embs)
        window_embeddings[bss] = win_embs
        station_emb_list.append(win_embs.mean(axis=0))
        valid_ids.append(bss)

    station_emb = np.stack(station_emb_list).astype(np.float32)
    elapsed = time.time() - t0

    return MethodResult(
        station_embeddings=station_emb,
        station_ids=valid_ids,
        window_embeddings=window_embeddings,
        elapsed_seconds=elapsed,
        method_name="TS2Vec",
    )
