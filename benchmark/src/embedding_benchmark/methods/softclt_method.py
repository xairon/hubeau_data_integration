"""
Méthode 5 : SoftCLT — Soft Contrastive Learning (ICLR 2024).

Même encoder que TS2Vec, loss modifiée avec soft assignments.
L'intégration repose sur le monkey-patching de la loss dans TS2Vec.

Si le patching échoue (API SoftCLT incompatible), on fallback sur TS2Vec
standard avec un warning — le notebook documentera l'issue.
"""

import time
import warnings
import torch
import numpy as np
from sklearn.preprocessing import StandardScaler
from typing import Dict

from ..config import cfg
from ..vendors.ts2vec.ts2vec import TS2Vec
from . import MethodResult


def _patch_softclt_loss() -> bool:
    """
    Remplace la loss de TS2Vec par celle de SoftCLT.

    SoftCLT fournit `hierarchical_contrastive_loss` comme drop-in replacement.
    On patche le module importé par ts2vec.py (qui utilise `from . import losses`).

    Returns True si le patch a réussi, False sinon.
    """
    try:
        from ..vendors.softclt import losses as softclt_losses
        from ..vendors.ts2vec import losses as ts2vec_losses

        if hasattr(softclt_losses, "hierarchical_contrastive_loss"):
            ts2vec_losses.hierarchical_contrastive_loss = softclt_losses.hierarchical_contrastive_loss
            return True
        else:
            warnings.warn("SoftCLT: aucune loss compatible trouvée, fallback TS2Vec standard")
            return False
    except Exception as e:
        warnings.warn(f"SoftCLT patch échoué: {e}. Fallback TS2Vec standard.")
        return False


def run(
    series: Dict[str, np.ndarray],
    dates: Dict[str, list],
    n_epochs: int = 200,
    lr: float = 1e-3,
    batch_size: int = 16,
    depth: int = 10,
) -> MethodResult:
    """Entraîne avec loss SoftCLT puis encode."""
    t0 = time.time()
    station_ids = sorted(series.keys())
    n_vars = len(cfg.piezo_cols)

    all_data = np.concatenate(list(series.values()))
    scaler = StandardScaler().fit(all_data)
    scaled = {bss: scaler.transform(arr) for bss, arr in series.items()}
    train_data = [scaled[bss].astype(np.float32) for bss in station_ids]

    # Patch la loss AVANT de créer le modèle
    patched = _patch_softclt_loss()
    method_name = "SoftCLT" if patched else "SoftCLT (fallback TS2Vec)"

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = TS2Vec(
        input_dims=n_vars,
        output_dims=cfg.embedding_dim,
        hidden_dims=cfg.embedding_dim,
        depth=depth,
        device=device,
    )

    model.fit(train_data, n_epochs=n_epochs, lr=lr, batch_size=batch_size, verbose=True)

    # Encoding identique à TS2Vec
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
        method_name=method_name,
    )
