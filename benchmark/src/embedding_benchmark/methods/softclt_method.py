"""SoftCLT : TS2Vec + Soft Contrastive Learning for Time Series."""

import time
import numpy as np
from sklearn.preprocessing import StandardScaler
from . import MethodResult
from ..config import cfg


def _patch_softclt_loss():
    """Monkey-patch TS2Vec loss with SoftCLT hierarchical contrastive loss."""
    from ..vendors.softclt.losses import hierarchical_contrastive_loss
    from ..vendors import ts2vec
    ts2vec.losses.hierarchical_contrastive_loss = hierarchical_contrastive_loss
    from ..vendors.ts2vec import ts2vec as ts2vec_module
    ts2vec_module.hierarchical_contrastive_loss = hierarchical_contrastive_loss


def run(series: dict[str, np.ndarray], dates: dict[str, np.ndarray],
        domains: dict[str, str] | None = None,
        n_epochs: int = 100, lr: float = 1e-3, batch_size: int = 32,
        depth: int = 10) -> MethodResult:
    """Train SoftCLT and compute embeddings.

    Args:
        series: {station_id: (T, n_vars)} arrays
        dates: {station_id: date_array}
        domains: {station_id: "piezo"|"hydro"} for per-domain normalization.
                 If None, single scaler for all.
    """
    from ..vendors.ts2vec.ts2vec import TS2Vec
    from ..data_loader import make_windows

    t0 = time.time()
    station_ids = list(series.keys())
    n_vars = next(iter(series.values())).shape[1]

    # ── Per-domain normalization ──
    if domains:
        scalers = {}
        scaled_series = {}
        for sid in station_ids:
            dom = domains[sid]
            if dom not in scalers:
                # Fit scaler on all series of this domain
                domain_data = np.concatenate([
                    series[s] for s in station_ids if domains[s] == dom
                ], axis=0)
                scalers[dom] = StandardScaler().fit(domain_data)
            scaled_series[sid] = scalers[dom].transform(series[sid])
    else:
        all_data = np.concatenate(list(series.values()), axis=0)
        scaler = StandardScaler().fit(all_data)
        scaled_series = {sid: scaler.transform(arr) for sid, arr in series.items()}

    # ── Windowing ──
    windowed, windowed_dates = make_windows(scaled_series, dates)
    if not windowed:
        raise ValueError("No station has enough data for windowing")

    # ── Prepare training data ──
    train_data = [scaled_series[sid].astype(np.float32) for sid in station_ids
                  if sid in windowed]
    # Filter to stations with windows
    station_ids = [sid for sid in station_ids if sid in windowed]

    # ── Patch loss + Train ──
    _patch_softclt_loss()

    model = TS2Vec(
        input_dims=n_vars,
        output_dims=cfg.embedding_dim,
        depth=depth,
        lr=lr,
        batch_size=batch_size,
        max_train_length=3000,
    )
    model.fit(train_data, n_epochs=n_epochs, verbose=True)

    # ── Encode ──
    station_embeddings = []
    window_embeddings = {}
    for sid in station_ids:
        windows = windowed[sid]  # (n_windows, window_size, n_vars)
        win_embs = []
        for w in windows:
            emb = model.encode(w[np.newaxis], encoding_window="full_series")
            win_embs.append(emb.squeeze())
        win_embs = np.stack(win_embs)
        window_embeddings[sid] = win_embs
        station_embeddings.append(win_embs.mean(axis=0))

    station_embeddings = np.stack(station_embeddings)
    domain_list = [domains.get(sid, "piezo") if domains else "piezo" for sid in station_ids]

    elapsed = time.time() - t0
    print(f"  SoftCLT done: {len(station_ids)} stations, {station_embeddings.shape[1]}d, {elapsed:.1f}s")

    return MethodResult(
        station_embeddings=station_embeddings,
        station_ids=station_ids,
        domains=domain_list,
        window_embeddings=window_embeddings,
        elapsed_seconds=elapsed,
        method_name="SoftCLT",
    )
