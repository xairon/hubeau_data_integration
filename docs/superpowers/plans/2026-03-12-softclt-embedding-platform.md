# SoftCLT Embedding Platform Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand benchmark to 2k stations (1k piezo + 1k hydro) with unified SoftCLT model and 5-page demo UI (clustering, similarity, anomaly detection, prediction, temporal analysis).

**Architecture:** Refactor benchmark internals from piezo-only `code_bss` to generic `station_id` + `domain`. Add hydro data loading. Train single SoftCLT on concatenated data. Build 5 Streamlit pages showcasing embedding applications.

**Tech Stack:** Python 3.11, SoftCLT (vendorized TS2Vec + patched loss), scikit-learn, Plotly, Streamlit, UMAP, PostgreSQL (read-only Gold tables)

**Spec:** `docs/superpowers/specs/2026-03-12-softclt-embedding-platform-design.md`

---

## Chunk 1: Core Infrastructure (config, data loader, evaluation refactor)

### Task 1: Extend config.py for hydro + unified mode

**Files:**
- Modify: `benchmark/src/embedding_benchmark/config.py`

- [ ] **Step 1: Add hydro columns and unified parameters**

```python
"""Configuration centralisée pour le benchmark."""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class BenchmarkConfig:
    """Configuration du benchmark, lue depuis .env ou valeurs par défaut."""

    # DB (lecture seule)
    pg_host: str = field(default_factory=lambda: os.getenv("PG_HOST", "localhost"))
    pg_port: int = field(default_factory=lambda: int(os.getenv("PG_PORT", "49502")))
    pg_db: str = field(default_factory=lambda: os.getenv("PG_DB", "postgres"))
    pg_user: str = field(default_factory=lambda: os.getenv("PG_USER", "postgres"))
    pg_password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", ""))

    # Benchmark params
    sample_size: int = field(default_factory=lambda: int(os.getenv("SAMPLE_SIZE", "300")))
    piezo_sample_size: int = field(default_factory=lambda: int(os.getenv("PIEZO_SAMPLE_SIZE", "1000")))
    hydro_sample_size: int = field(default_factory=lambda: int(os.getenv("HYDRO_SAMPLE_SIZE", "1000")))
    window_size: int = field(default_factory=lambda: int(os.getenv("WINDOW_SIZE", "365")))
    stride: int = field(default_factory=lambda: int(os.getenv("STRIDE", "90")))
    embedding_dim: int = field(default_factory=lambda: int(os.getenv("EMBEDDING_DIM", "320")))
    seed: int = field(default_factory=lambda: int(os.getenv("SEED", "42")))

    # Variables piézo (measurement + ERA5)
    piezo_cols: list[str] = field(default_factory=lambda: [
        "niveau_nappe_eau", "temperature_2m", "total_precipitation", "potential_evaporation"
    ])

    # Variables hydro (measurement + ERA5)
    hydro_cols: list[str] = field(default_factory=lambda: [
        "resultat_obs_elab", "temperature_2m", "total_precipitation", "potential_evaporation"
    ])

    # ERA5 columns (shared)
    era5_cols: list[str] = field(default_factory=lambda: [
        "temperature_2m", "total_precipitation", "potential_evaporation"
    ])

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}"

    @property
    def results_dir(self):
        from pathlib import Path
        d = Path(__file__).parent.parent.parent / "results"
        d.mkdir(parents=True, exist_ok=True)
        return d

    @property
    def embeddings_dir(self):
        d = self.results_dir / "embeddings"
        d.mkdir(parents=True, exist_ok=True)
        return d

    @property
    def windows_dir(self):
        d = self.results_dir / "windows"
        d.mkdir(parents=True, exist_ok=True)
        return d

    @property
    def metrics_dir(self):
        d = self.results_dir / "metrics"
        d.mkdir(parents=True, exist_ok=True)
        return d

    @property
    def models_dir(self):
        d = self.results_dir / "models"
        d.mkdir(parents=True, exist_ok=True)
        return d


# Singleton
cfg = BenchmarkConfig()
```

- [ ] **Step 2: Verify import works**

Run: `cd benchmark && .venv/bin/python -c "from embedding_benchmark.config import cfg; print(cfg.hydro_cols, cfg.piezo_sample_size)"`

- [ ] **Step 3: Commit**

```bash
git add benchmark/src/embedding_benchmark/config.py
git commit -m "feat(benchmark): extend config with hydro columns and unified parameters"
```

---

### Task 2: Refactor MethodResult to use generic station_id

**Files:**
- Modify: `benchmark/src/embedding_benchmark/methods/__init__.py`

- [ ] **Step 1: Update MethodResult dataclass**

```python
"""Résultats d'une méthode d'embedding."""

from dataclasses import dataclass
import numpy as np


@dataclass
class MethodResult:
    """Résultat d'une méthode d'embedding.

    Attributes:
        station_embeddings: (n_stations, dim) array of station-level embeddings
        station_ids: list of station identifiers (code_bss for piezo, code_station for hydro)
        domains: list of domain labels ("piezo" or "hydro") per station
        window_embeddings: {station_id: (n_windows, dim)} dict of window-level embeddings
        elapsed_seconds: wall-clock time for the method
        method_name: name of the method
    """
    station_embeddings: np.ndarray
    station_ids: list[str]
    domains: list[str]
    window_embeddings: dict[str, np.ndarray]
    elapsed_seconds: float
    method_name: str
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/methods/__init__.py
git commit -m "refactor(benchmark): MethodResult uses generic station_ids + domains"
```

---

### Task 3: Refactor data_loader.py for piezo + hydro

**Files:**
- Modify: `benchmark/src/embedding_benchmark/data_loader.py`

- [ ] **Step 1: Rewrite data_loader with dual-domain support**

```python
"""Chargement des données piézo et hydro depuis les tables Gold."""

import numpy as np
import pandas as pd
import psycopg2
from .config import cfg


# ── Piezo ──────────────────────────────────────────────────────────────────


def get_eligible_piezo_stations(min_days: int = 730) -> pd.DataFrame:
    """Stations piézo éligibles (≥min_days jours, dernière mesure ≥2024)."""
    query = """
        SELECT code_bss AS station_id, nature_eh, code_departement, code_region,
               COUNT(*) AS n_days, MAX(date) AS last_date
        FROM gold.hubeau_daily_chroniques
        GROUP BY code_bss, nature_eh, code_departement, code_region
        HAVING COUNT(*) >= %(min_days)s AND MAX(date) >= '2024-01-01'
        ORDER BY n_days DESC
    """
    with psycopg2.connect(cfg.dsn) as conn:
        df = pd.read_sql(query, conn, params={"min_days": min_days})
    df["domain"] = "piezo"
    return df


def get_eligible_hydro_stations(min_days: int = 730) -> pd.DataFrame:
    """Stations hydro éligibles (QmnJ, ≥min_days jours, dernière mesure ≥2024)."""
    query = """
        SELECT code_station AS station_id, type_site, code_departement, code_region,
               COUNT(*) AS n_days, MAX(date) AS last_date
        FROM gold.hydro_daily_chroniques
        WHERE grandeur_hydro_elab = 'QmnJ'
        GROUP BY code_station, type_site, code_departement, code_region
        HAVING COUNT(*) >= %(min_days)s AND MAX(date) >= '2024-01-01'
        ORDER BY n_days DESC
    """
    with psycopg2.connect(cfg.dsn) as conn:
        df = pd.read_sql(query, conn, params={"min_days": min_days})
    df["domain"] = "hydro"
    return df


# ── Sampling ───────────────────────────────────────────────────────────────


def sample_stations(eligible: pd.DataFrame, n: int, stratify_col: str,
                    seed: int | None = None) -> pd.DataFrame:
    """Échantillonnage stratifié par stratify_col."""
    seed = seed or cfg.seed
    if len(eligible) <= n:
        return eligible

    # Proportional allocation per stratum
    counts = eligible[stratify_col].value_counts()
    fracs = counts / counts.sum()
    samples = []
    for value, frac in fracs.items():
        stratum = eligible[eligible[stratify_col] == value]
        k = max(1, round(frac * n))
        k = min(k, len(stratum))
        samples.append(stratum.sample(n=k, random_state=seed))

    result = pd.concat(samples).head(n)  # trim to exact n
    return result.reset_index(drop=True)


# ── Series Loading ─────────────────────────────────────────────────────────


def load_piezo_series(station_ids: list[str]) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    """Charge les séries piézo multivar. Returns (series, dates) dicts keyed by station_id."""
    placeholders = ",".join(["%s"] * len(station_ids))
    query = f"""
        SELECT code_bss AS station_id, date,
               niveau_nappe_eau, temperature_2m, total_precipitation, potential_evaporation
        FROM gold.hubeau_daily_chroniques
        WHERE code_bss IN ({placeholders})
        ORDER BY code_bss, date
    """
    with psycopg2.connect(cfg.dsn) as conn:
        df = pd.read_sql(query, conn, params=tuple(station_ids))
    return _build_series_dicts(df, cfg.piezo_cols)


def load_hydro_series(station_ids: list[str]) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    """Charge les séries hydro multivar (QmnJ only). Returns (series, dates) dicts."""
    placeholders = ",".join(["%s"] * len(station_ids))
    query = f"""
        SELECT code_station AS station_id, date,
               resultat_obs_elab, temperature_2m, total_precipitation, potential_evaporation
        FROM gold.hydro_daily_chroniques
        WHERE code_station IN ({placeholders})
          AND grandeur_hydro_elab = 'QmnJ'
        ORDER BY code_station, date
    """
    with psycopg2.connect(cfg.dsn) as conn:
        df = pd.read_sql(query, conn, params=tuple(station_ids))
    return _build_series_dicts(df, cfg.hydro_cols)


def _build_series_dicts(df: pd.DataFrame, cols: list[str]) -> tuple[dict, dict]:
    """Convert a DataFrame with station_id, date, and value columns into series/dates dicts."""
    series = {}
    dates = {}
    for sid, group in df.groupby("station_id"):
        group = group.sort_values("date")
        arr = group[cols].values.astype(np.float32)
        # Interpolate NaN, then fill remaining with 0
        mask = np.isnan(arr)
        if mask.any():
            for col_idx in range(arr.shape[1]):
                col = arr[:, col_idx]
                nans = np.isnan(col)
                if nans.any() and not nans.all():
                    col[nans] = np.interp(
                        np.flatnonzero(nans), np.flatnonzero(~nans), col[~nans]
                    )
            arr = np.nan_to_num(arr, nan=0.0)
        series[sid] = arr
        dates[sid] = group["date"].values
    return series, dates


# ── Unified Loading ────────────────────────────────────────────────────────


def load_unified_data(piezo_n: int | None = None, hydro_n: int | None = None):
    """Load and sample piezo + hydro stations. Returns (sample_df, series, dates).

    sample_df has columns: station_id, domain, + domain-specific metadata.
    series/dates are dicts keyed by station_id.
    """
    piezo_n = piezo_n or cfg.piezo_sample_size
    hydro_n = hydro_n or cfg.hydro_sample_size

    # Eligible stations
    piezo_eligible = get_eligible_piezo_stations()
    hydro_eligible = get_eligible_hydro_stations()

    print(f"  Piézo: {len(piezo_eligible)} éligibles")
    print(f"  Hydro: {len(hydro_eligible)} éligibles")

    # Sample
    piezo_sample = sample_stations(piezo_eligible, piezo_n, "nature_eh")
    hydro_sample = sample_stations(hydro_eligible, hydro_n, "type_site")

    print(f"  Piézo: {len(piezo_sample)} échantillonnées")
    print(f"  Hydro: {len(hydro_sample)} échantillonnées")

    # Load series
    piezo_series, piezo_dates = load_piezo_series(piezo_sample["station_id"].tolist())
    hydro_series, hydro_dates = load_hydro_series(hydro_sample["station_id"].tolist())

    # Merge
    sample_df = pd.concat([piezo_sample, hydro_sample], ignore_index=True)
    all_series = {**piezo_series, **hydro_series}
    all_dates = {**piezo_dates, **hydro_dates}

    print(f"  Total: {len(all_series)} séries chargées")
    return sample_df, all_series, all_dates


# ── Windowing (unchanged) ─────────────────────────────────────────────────


def make_windows(series: dict[str, np.ndarray], dates: dict[str, np.ndarray],
                 window_size: int | None = None, stride: int | None = None):
    """Découpe les séries en fenêtres glissantes."""
    window_size = window_size or cfg.window_size
    stride = stride or cfg.stride
    windowed_series = {}
    windowed_dates = {}

    for sid, arr in series.items():
        n = len(arr)
        if n < window_size:
            continue
        windows = []
        date_windows = []
        d = dates[sid]
        for start in range(0, n - window_size + 1, stride):
            windows.append(arr[start:start + window_size])
            date_windows.append((d[start], d[start + window_size - 1]))
        if windows:
            windowed_series[sid] = np.stack(windows)
            windowed_dates[sid] = date_windows

    return windowed_series, windowed_dates
```

- [ ] **Step 2: Verify data loading works**

Run: `cd benchmark && .venv/bin/python -c "
from embedding_benchmark.data_loader import get_eligible_piezo_stations, get_eligible_hydro_stations
p = get_eligible_piezo_stations()
h = get_eligible_hydro_stations()
print(f'Piezo: {len(p)} eligible, columns: {list(p.columns)}')
print(f'Hydro: {len(h)} eligible, columns: {list(h.columns)}')
"`

Expected: counts for both domains, `station_id` and `domain` columns present.

- [ ] **Step 3: Commit**

```bash
git add benchmark/src/embedding_benchmark/data_loader.py
git commit -m "refactor(benchmark): dual-domain data loader (piezo + hydro)"
```

---

### Task 4: Refactor evaluation.py for domain-aware metrics

**Files:**
- Modify: `benchmark/src/embedding_benchmark/evaluation.py`

- [ ] **Step 1: Rewrite evaluation with domain-aware metrics and window saving**

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/evaluation.py
git commit -m "refactor(benchmark): domain-aware evaluation metrics + window serialization"
```

---

### Task 5: Update softclt_method.py to return domains

**Files:**
- Modify: `benchmark/src/embedding_benchmark/methods/softclt_method.py`

- [ ] **Step 1: Update SoftCLT run() to accept and return domains**

The key change: accept `domains` list, pass it through to MethodResult. Also apply per-domain normalization.

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/methods/softclt_method.py
git commit -m "feat(benchmark): SoftCLT with per-domain normalization and domains output"
```

---

## Chunk 2: Run Script + New Modules (anomaly, prediction)

### Task 6: Create run_softclt.py unified runner

**Files:**
- Create: `benchmark/scripts/run_softclt.py`

- [ ] **Step 1: Write unified runner script**

```python
"""Run SoftCLT on unified piezo + hydro data.

Usage:
    cd benchmark
    python scripts/run_softclt.py                          # Default: 1k piezo + 1k hydro
    python scripts/run_softclt.py --piezo 500 --hydro 500   # Custom sizes
    python scripts/run_softclt.py --piezo 50 --hydro 50     # Quick test
"""

import argparse
import json
import sys
import time
import torch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from embedding_benchmark.config import cfg
from embedding_benchmark.data_loader import load_unified_data
from embedding_benchmark.methods.softclt_method import run as run_softclt
from embedding_benchmark.evaluation import (
    run_full_evaluation, save_embeddings, save_window_embeddings,
)


def main():
    parser = argparse.ArgumentParser(description="SoftCLT unified benchmark (piezo + hydro)")
    parser.add_argument("--piezo", type=int, default=None, help=f"Piezo sample size (default: {cfg.piezo_sample_size})")
    parser.add_argument("--hydro", type=int, default=None, help=f"Hydro sample size (default: {cfg.hydro_sample_size})")
    parser.add_argument("--epochs", type=int, default=100, help="Training epochs (default: 100)")
    args = parser.parse_args()

    piezo_n = args.piezo or cfg.piezo_sample_size
    hydro_n = args.hydro or cfg.hydro_sample_size

    print(f"{'=' * 60}")
    print(f"SOFTCLT UNIFIED BENCHMARK — {piezo_n} piézo + {hydro_n} hydro")
    print(f"{'=' * 60}\n")

    # 1. Load data
    print("Chargement des données...")
    sample_df, series, dates = load_unified_data(piezo_n, hydro_n)

    # Build domain map
    domains_map = dict(zip(sample_df["station_id"], sample_df["domain"]))

    # 2. Train SoftCLT
    print(f"\n{'=' * 60}")
    print(f"TRAINING SOFTCLT ({len(series)} stations, {args.epochs} epochs)")
    print(f"{'=' * 60}\n")

    result = run_softclt(series, dates, domains=domains_map, n_epochs=args.epochs)

    # 3. Evaluate
    print(f"\n{'=' * 60}")
    print("ÉVALUATION")
    print(f"{'=' * 60}\n")

    metrics, labels = run_full_evaluation(
        result.station_embeddings,
        result.station_ids,
        result.domains,
        sample_df,
        window_embeddings=result.window_embeddings,
        method_name="SoftCLT_unified",
    )

    # 4. Save
    save_embeddings(
        result.station_embeddings, result.station_ids, result.domains,
        labels, "SoftCLT_unified", sample_df,
    )
    save_window_embeddings(result.window_embeddings, domains_map, "SoftCLT_unified")

    # Save model
    from embedding_benchmark.methods.softclt_method import _patch_softclt_loss
    model_path = cfg.models_dir / "softclt_unified.pt"
    # Model is not easily serializable (TS2Vec), save embeddings instead
    print(f"\n  Model artifacts saved to {cfg.results_dir}/")

    # 5. Summary
    print(f"\n{'=' * 60}")
    print("RÉSUMÉ")
    print(f"{'=' * 60}\n")

    for k, v in metrics.items():
        print(f"  {k}: {v}")

    print(f"\n  Temps total: {result.elapsed_seconds:.1f}s")
    print(f"\n  Pour lancer l'UI : cd benchmark && streamlit run app/app.py")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/scripts/run_softclt.py
git commit -m "feat(benchmark): unified SoftCLT runner (piezo + hydro)"
```

---

### Task 7: Create anomaly.py module

**Files:**
- Create: `benchmark/src/embedding_benchmark/anomaly.py`

- [ ] **Step 1: Write anomaly detection module**

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/anomaly.py
git commit -m "feat(benchmark): anomaly detection module (IsolationForest + LOF)"
```

---

### Task 8: Create prediction.py module

**Files:**
- Create: `benchmark/src/embedding_benchmark/prediction.py`

- [ ] **Step 1: Write downstream prediction module**

```python
"""Prédiction downstream avec embeddings comme features."""

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix
from sklearn.preprocessing import LabelEncoder


def run_classification(embeddings: np.ndarray, labels: list[str],
                       task_name: str, test_size: float = 0.2,
                       seed: int = 42) -> dict:
    """Run RF + LogReg classification on embeddings. Returns metrics dict."""
    le = LabelEncoder()
    y = le.fit_transform(labels)
    classes = le.classes_

    # Filter classes with too few samples for stratified split
    class_counts = np.bincount(y)
    valid_classes = np.where(class_counts >= 2)[0]
    if len(valid_classes) < 2:
        return {"task": task_name, "error": "Too few classes with ≥2 samples"}

    valid_mask = np.isin(y, valid_classes)
    X = embeddings[valid_mask]
    y = y[valid_mask]
    classes = classes[valid_classes]

    # Re-encode after filtering
    le2 = LabelEncoder()
    y = le2.fit_transform([classes[yi] for yi in y] if len(valid_classes) < len(le.classes_) else le.inverse_transform(y))
    classes = le2.classes_

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=seed, stratify=y
    )

    results = {"task": task_name, "n_samples": len(X), "n_classes": len(classes),
               "classes": classes.tolist()}

    for name, clf in [("random_forest", RandomForestClassifier(n_estimators=100, random_state=seed)),
                      ("logistic_regression", LogisticRegression(max_iter=1000, random_state=seed))]:
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred, average="weighted")
        cm = confusion_matrix(y_test, y_pred)

        results[name] = {
            "accuracy": round(acc, 4),
            "f1_weighted": round(f1, 4),
            "confusion_matrix": cm.tolist(),
        }

        # Feature importance (RF only)
        if name == "random_forest":
            results["feature_importance"] = clf.feature_importances_.tolist()

    return results


def run_all_predictions(embeddings: np.ndarray, station_ids: list[str],
                        domains: list[str], meta_df: pd.DataFrame) -> list[dict]:
    """Run all downstream prediction tasks."""
    results = []
    meta_map = meta_df.set_index("station_id")

    # Task 1: Predict domain (piezo vs hydro) — binary
    results.append(run_classification(embeddings, domains, "domain"))

    # Task 2: Predict nature_eh (piezo only)
    piezo_mask = np.array(domains) == "piezo"
    if piezo_mask.sum() > 10 and "nature_eh" in meta_df.columns:
        piezo_ids = [s for s, d in zip(station_ids, domains) if d == "piezo"]
        nature_labels = [meta_map.loc[s, "nature_eh"] if s in meta_map.index else "unknown"
                         for s in piezo_ids]
        # Filter out unknown/NaN
        valid = [(e, l) for e, l, m in zip(embeddings[piezo_mask], nature_labels, [True]*piezo_mask.sum())
                 if l != "unknown" and pd.notna(l)]
        if len(valid) > 10:
            X = np.stack([v[0] for v in valid])
            y = [v[1] for v in valid]
            results.append(run_classification(X, y, "nature_eh"))

    # Task 3: Predict region
    if "code_region" in meta_df.columns:
        region_labels = [meta_map.loc[s, "code_region"] if s in meta_map.index else "unknown"
                         for s in station_ids]
        valid = [(e, l) for e, l in zip(embeddings, region_labels)
                 if l != "unknown" and pd.notna(l)]
        if len(valid) > 10:
            X = np.stack([v[0] for v in valid])
            y = [v[1] for v in valid]
            results.append(run_classification(X, y, "region"))

    return results
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/prediction.py
git commit -m "feat(benchmark): downstream prediction module (domain, nature_eh, region)"
```

---

## Chunk 3: Streamlit UI (5 pages)

### Task 9: Update UI components and app.py

**Files:**
- Modify: `benchmark/src/embedding_benchmark/ui/components.py`
- Modify: `benchmark/app/app.py`

- [ ] **Step 1: Rewrite components.py for unified data**

```python
"""Composants UI réutilisables pour le benchmark Streamlit."""

import json
import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path

from embedding_benchmark.config import cfg


def load_embeddings(name: str = "SoftCLT_unified") -> pd.DataFrame:
    """Load station embeddings from parquet."""
    path = cfg.embeddings_dir / f"{name}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_window_embeddings(name: str = "SoftCLT_unified") -> pd.DataFrame:
    """Load window-level embeddings from parquet."""
    path = cfg.windows_dir / f"{name}_windows.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_metrics(name: str = "SoftCLT_unified") -> dict:
    """Load metrics JSON."""
    path = cfg.metrics_dir / f"{name}.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def get_embedding_columns(df: pd.DataFrame) -> list[str]:
    """Return embedding dimension column names."""
    return [c for c in df.columns if c.startswith("emb_")]


def get_embedding_matrix(df: pd.DataFrame) -> np.ndarray:
    """Extract embedding matrix from DataFrame."""
    return df[get_embedding_columns(df)].values


@st.cache_data(ttl=3600)
def compute_umap(name: str = "SoftCLT_unified", n_components: int = 2) -> np.ndarray:
    """Compute cached UMAP projection."""
    import umap
    df = load_embeddings(name)
    if df.empty:
        return np.array([])
    X = get_embedding_matrix(df)
    reducer = umap.UMAP(n_components=n_components, random_state=42, metric="cosine")
    return reducer.fit_transform(X)


def has_results(name: str = "SoftCLT_unified") -> bool:
    """Check if results exist for the given run."""
    return (cfg.embeddings_dir / f"{name}.parquet").exists()
```

- [ ] **Step 2: Rewrite app.py**

```python
"""SoftCLT Embedding Platform — Piézo + Hydro."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import streamlit as st
from embedding_benchmark.ui.components import has_results, load_metrics

st.set_page_config(page_title="SoftCLT Embeddings", page_icon="🌊", layout="wide")

st.title("SoftCLT Embedding Platform")
st.markdown("Exploration des embeddings de séries temporelles hydrologiques (piézométrie + hydrométrie)")

if not has_results():
    st.error("Aucun résultat trouvé. Lancez d'abord le benchmark :")
    st.code("cd benchmark && python scripts/run_softclt.py --piezo 50 --hydro 50")
    st.stop()

metrics = load_metrics()
if metrics:
    cols = st.columns(4)
    cols[0].metric("Stations", metrics.get("n_stations", "?"))
    cols[1].metric("Clusters", metrics.get("n_clusters", "?"))
    cols[2].metric("Silhouette", metrics.get("silhouette", "?"))
    cols[3].metric("Stabilité temporelle", metrics.get("temporal_stability", "?"))

    st.markdown("---")
    st.markdown("Naviguez dans les pages ci-dessous pour explorer les embeddings.")
```

- [ ] **Step 3: Commit**

```bash
git add benchmark/src/embedding_benchmark/ui/components.py benchmark/app/app.py
git commit -m "refactor(benchmark): UI components for unified SoftCLT data"
```

---

### Task 10: Page 1 — Clustering & Exploration

**Files:**
- Create: `benchmark/app/pages/1_clustering.py`

- [ ] **Step 1: Write clustering page**

```python
"""Page 1 : Clustering & Exploration UMAP."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st
import plotly.express as px
import numpy as np
from embedding_benchmark.ui.components import (
    load_embeddings, compute_umap, get_embedding_matrix, has_results,
)
from embedding_benchmark.evaluation import cluster_hdbscan, eval_silhouette

st.set_page_config(page_title="Clustering", page_icon="🔬", layout="wide")
st.title("Clustering & Exploration")

if not has_results():
    st.error("Aucun résultat. Lancez `python scripts/run_softclt.py` d'abord.")
    st.stop()

df = load_embeddings()
if df.empty:
    st.stop()

# Sidebar controls
st.sidebar.header("Paramètres")
min_cluster_size = st.sidebar.slider("HDBSCAN min_cluster_size", 3, 50, 10)
min_samples = st.sidebar.slider("HDBSCAN min_samples", 1, 30, 5)
color_by = st.sidebar.selectbox("Colorer par", ["cluster", "domain", "nature_eh", "type_site", "code_departement", "code_region"])
domain_filter = st.sidebar.multiselect("Filtrer par domaine", ["piezo", "hydro"], default=["piezo", "hydro"])

# Filter
mask = df["domain"].isin(domain_filter)
df_filtered = df[mask].reset_index(drop=True)

if len(df_filtered) < 5:
    st.warning("Trop peu de stations après filtrage.")
    st.stop()

# Re-cluster on filtered data
X = get_embedding_matrix(df_filtered)
labels = cluster_hdbscan(X, min_cluster_size=min_cluster_size, min_samples=min_samples)
df_filtered["cluster"] = labels.astype(str)
sil = eval_silhouette(X, labels)

# UMAP
umap_coords = compute_umap()
umap_filtered = umap_coords[mask] if len(umap_coords) == len(df) else None

if umap_filtered is None or len(umap_filtered) != len(df_filtered):
    import umap as umap_lib
    reducer = umap_lib.UMAP(n_components=2, random_state=42, metric="cosine")
    umap_filtered = reducer.fit_transform(X)

df_filtered["umap_x"] = umap_filtered[:, 0]
df_filtered["umap_y"] = umap_filtered[:, 1]

# Color column
if color_by == "cluster":
    color_col = "cluster"
elif color_by in df_filtered.columns:
    color_col = color_by
    df_filtered[color_col] = df_filtered[color_col].fillna("inconnu").astype(str)
else:
    st.warning(f"Colonne '{color_by}' non disponible pour ce domaine.")
    color_col = "domain"

# Stats
n_clusters = len(set(labels[labels >= 0]))
n_noise = int((labels == -1).sum())

col1, col2, col3, col4 = st.columns(4)
col1.metric("Stations", len(df_filtered))
col2.metric("Clusters", n_clusters)
col3.metric("Noise", f"{n_noise} ({100*n_noise/len(df_filtered):.0f}%)")
col4.metric("Silhouette", f"{sil:.3f}" if sil > -1 else "N/A")

# UMAP plot
fig = px.scatter(
    df_filtered, x="umap_x", y="umap_y", color=color_col,
    hover_data=["station_id", "domain", "cluster"],
    title=f"UMAP — coloré par {color_by}",
    width=900, height=600,
)
fig.update_traces(marker=dict(size=5, opacity=0.7))
fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=-0.2))
st.plotly_chart(fig, use_container_width=True)

# Cluster distribution
if n_clusters > 0:
    st.subheader("Distribution des clusters")
    dist = df_filtered[df_filtered["cluster"] != "-1"].groupby(["cluster", "domain"]).size().reset_index(name="count")
    fig2 = px.bar(dist, x="cluster", y="count", color="domain", barmode="group",
                  title="Nombre de stations par cluster et domaine")
    st.plotly_chart(fig2, use_container_width=True)
```

- [ ] **Step 2: Remove old comparison page**

Delete `benchmark/app/pages/1_comparison.py` (replaced by clustering page).

- [ ] **Step 3: Commit**

```bash
git add benchmark/app/pages/1_clustering.py
git rm benchmark/app/pages/1_comparison.py 2>/dev/null; true
git commit -m "feat(benchmark): page 1 — clustering & UMAP exploration"
```

---

### Task 11: Page 2 — Similarity Search

**Files:**
- Create: `benchmark/app/pages/2_similarity.py`

- [ ] **Step 1: Write similarity page**

```python
"""Page 2 : Recherche de similarité (kNN)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from embedding_benchmark.ui.components import (
    load_embeddings, compute_umap, get_embedding_matrix, has_results,
)
from embedding_benchmark.data_loader import load_piezo_series, load_hydro_series
from embedding_benchmark.config import cfg

st.set_page_config(page_title="Similarité", page_icon="🔍", layout="wide")
st.title("Recherche de similarité")

if not has_results():
    st.error("Aucun résultat.")
    st.stop()

df = load_embeddings()
if df.empty:
    st.stop()

X = get_embedding_matrix(df)

# Station selector
station_options = [f"[{row['domain']}] {row['station_id']}" for _, row in df.iterrows()]
selected = st.selectbox("Station de référence", station_options)
selected_idx = station_options.index(selected)
selected_id = df.iloc[selected_idx]["station_id"]
selected_domain = df.iloc[selected_idx]["domain"]

# K slider
max_k = min(50, len(df) - 1)
k = st.slider("Nombre de voisins (K)", 1, max(1, max_k), min(10, max_k))

# kNN search
nn = NearestNeighbors(n_neighbors=min(k + 1, len(df)), metric="cosine")
nn.fit(X)
distances, indices = nn.kneighbors(X[selected_idx:selected_idx + 1])

neighbors = []
for rank, (idx, dist) in enumerate(zip(indices[0][1:], distances[0][1:])):
    row = df.iloc[idx]
    neighbors.append({
        "Rang": rank + 1,
        "Station": row["station_id"],
        "Domaine": row["domain"],
        "Distance cosine": round(dist, 4),
        **{c: row.get(c, "") for c in ["nature_eh", "type_site", "code_departement"] if c in df.columns},
    })

# Display
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader(f"Référence: {selected_id}")
    ref_meta = df.iloc[selected_idx]
    for c in ["domain", "nature_eh", "type_site", "code_departement", "code_region"]:
        if c in ref_meta and pd.notna(ref_meta[c]):
            st.write(f"**{c}**: {ref_meta[c]}")

    # Cross-domain count
    neighbor_df = pd.DataFrame(neighbors)
    cross = neighbor_df[neighbor_df["Domaine"] != selected_domain]
    if len(cross) > 0:
        st.success(f"🔀 {len(cross)}/{k} voisins cross-domaine !")

    st.dataframe(neighbor_df, use_container_width=True, hide_index=True)

with col2:
    # UMAP with highlight
    umap_coords = compute_umap()
    if len(umap_coords) == len(df):
        df_plot = df.copy()
        df_plot["umap_x"] = umap_coords[:, 0]
        df_plot["umap_y"] = umap_coords[:, 1]

        # Mark reference + neighbors
        df_plot["role"] = "other"
        df_plot.loc[selected_idx, "role"] = "reference"
        for idx in indices[0][1:]:
            df_plot.loc[idx, "role"] = "neighbor"

        fig = px.scatter(df_plot, x="umap_x", y="umap_y", color="role",
                         color_discrete_map={"other": "lightgrey", "reference": "red", "neighbor": "blue"},
                         hover_data=["station_id", "domain"],
                         title="UMAP — référence (rouge) et voisins (bleu)",
                         width=800, height=600)
        fig.update_traces(marker=dict(size=4, opacity=0.5), selector=dict(name="other"))
        fig.update_traces(marker=dict(size=10, opacity=1.0), selector=dict(name="reference"))
        fig.update_traces(marker=dict(size=8, opacity=0.9), selector=dict(name="neighbor"))
        st.plotly_chart(fig, use_container_width=True)

# Time series comparison
st.subheader("Comparaison des séries temporelles")
n_show = st.slider("Nombre de voisins à afficher", 1, min(k, 5), min(3, k), key="ts_show")

stations_to_load = [selected_id] + [neighbors[i]["Station"] for i in range(n_show)]
domains_to_load = [selected_domain] + [neighbors[i]["Domaine"] for i in range(n_show)]

# Load and plot time series (normalized for comparison)
fig_ts = go.Figure()
for sid, dom in zip(stations_to_load, domains_to_load):
    try:
        if dom == "piezo":
            s, _ = load_piezo_series([sid])
            col_name = "niveau_nappe_eau"
        else:
            s, _ = load_hydro_series([sid])
            col_name = "resultat_obs_elab"
        if sid in s:
            arr = s[sid][:, 0]  # first column = measurement
            # Z-score normalize for comparison
            arr = (arr - arr.mean()) / (arr.std() + 1e-8)
            label = f"{'⭐ ' if sid == selected_id else ''}{sid} [{dom}]"
            fig_ts.add_trace(go.Scatter(y=arr[-365*2:], name=label, mode="lines",
                                        line=dict(width=3 if sid == selected_id else 1)))
    except Exception:
        pass

fig_ts.update_layout(title="Séries normalisées (2 dernières années)", height=400,
                     xaxis_title="Jours", yaxis_title="Z-score")
st.plotly_chart(fig_ts, use_container_width=True)
```

- [ ] **Step 2: Remove old pages 2 and 3**

Delete `benchmark/app/pages/2_embeddings_explorer.py` and `benchmark/app/pages/3_station_similarity.py`.

- [ ] **Step 3: Commit**

```bash
git add benchmark/app/pages/2_similarity.py
git rm benchmark/app/pages/2_embeddings_explorer.py benchmark/app/pages/3_station_similarity.py 2>/dev/null; true
git commit -m "feat(benchmark): page 2 — kNN similarity search with cross-domain + time series"
```

---

### Task 12: Page 3 — Anomaly Detection

**Files:**
- Create: `benchmark/app/pages/3_anomaly_detection.py`

- [ ] **Step 1: Write anomaly detection page**

```python
"""Page 3 : Détection d'anomalies."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
from embedding_benchmark.ui.components import (
    load_embeddings, compute_umap, get_embedding_matrix, has_results,
)
from embedding_benchmark.anomaly import (
    detect_anomalies_iforest, detect_anomalies_lof, build_anomaly_table,
)
from embedding_benchmark.data_loader import load_piezo_series, load_hydro_series

st.set_page_config(page_title="Anomalies", page_icon="⚠️", layout="wide")
st.title("Détection d'anomalies")

if not has_results():
    st.error("Aucun résultat.")
    st.stop()

df = load_embeddings()
if df.empty:
    st.stop()

X = get_embedding_matrix(df)
station_ids = df["station_id"].tolist()
domains = df["domain"].tolist()

# Controls
st.sidebar.header("Paramètres")
method = st.sidebar.selectbox("Méthode", ["Isolation Forest", "LOF"])
contamination = st.sidebar.slider("Contamination (%)", 1, 20, 5) / 100

# Detect
if method == "Isolation Forest":
    scores = detect_anomalies_iforest(X, contamination=contamination)
else:
    scores = detect_anomalies_lof(X, contamination=contamination)

threshold = np.percentile(scores, contamination * 100)
anomaly_mask = scores <= threshold

# Stats
n_anomalies = anomaly_mask.sum()
col1, col2, col3 = st.columns(3)
col1.metric("Stations totales", len(df))
col2.metric("Anomalies détectées", n_anomalies)
col3.metric("Taux", f"{100*n_anomalies/len(df):.1f}%")

# UMAP with anomalies
umap_coords = compute_umap()
if len(umap_coords) == len(df):
    df_plot = df.copy()
    df_plot["umap_x"] = umap_coords[:, 0]
    df_plot["umap_y"] = umap_coords[:, 1]
    df_plot["anomaly"] = np.where(anomaly_mask, "anomalie", "normal")
    df_plot["score"] = scores

    fig = px.scatter(df_plot, x="umap_x", y="umap_y", color="anomaly",
                     color_discrete_map={"normal": "lightblue", "anomalie": "red"},
                     hover_data=["station_id", "domain", "score"],
                     title=f"UMAP — {method} (contamination={contamination:.0%})",
                     width=900, height=600)
    fig.update_traces(marker=dict(size=4, opacity=0.5), selector=dict(name="normal"))
    fig.update_traces(marker=dict(size=10, opacity=1.0), selector=dict(name="anomalie"))
    st.plotly_chart(fig, use_container_width=True)

# Anomaly table
st.subheader("Stations anomaliques")
anomaly_table = build_anomaly_table(X, station_ids, domains, scores, contamination)
st.dataframe(anomaly_table, use_container_width=True, hide_index=True)

# Compare anomaly vs normal
if len(anomaly_table) > 0:
    st.subheader("Comparaison : anomalie vs station normale la plus proche")
    selected_anom = st.selectbox("Station anomalique",
                                  anomaly_table["station_id"].tolist())
    row = anomaly_table[anomaly_table["station_id"] == selected_anom].iloc[0]
    normal_id = row["nearest_normal_id"]

    if normal_id:
        fig_ts = go.Figure()
        for sid, label, color in [(selected_anom, f"Anomalie: {selected_anom}", "red"),
                                   (normal_id, f"Normal: {normal_id}", "blue")]:
            dom = row["domain"] if sid == selected_anom else row["nearest_normal_domain"]
            try:
                if dom == "piezo":
                    s, _ = load_piezo_series([sid])
                else:
                    s, _ = load_hydro_series([sid])
                if sid in s:
                    arr = s[sid][:, 0]
                    arr = (arr - arr.mean()) / (arr.std() + 1e-8)
                    fig_ts.add_trace(go.Scatter(y=arr[-365*2:], name=label, mode="lines",
                                                line=dict(color=color, width=2)))
            except Exception:
                pass

        fig_ts.update_layout(title="Séries normalisées (2 dernières années)", height=400)
        st.plotly_chart(fig_ts, use_container_width=True)
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/app/pages/3_anomaly_detection.py
git commit -m "feat(benchmark): page 3 — anomaly detection (IsolationForest + LOF)"
```

---

### Task 13: Page 4 — Downstream Prediction

**Files:**
- Create: `benchmark/app/pages/4_prediction.py`

- [ ] **Step 1: Write prediction page**

```python
"""Page 4 : Prédiction downstream."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
from embedding_benchmark.ui.components import (
    load_embeddings, get_embedding_matrix, has_results,
)
from embedding_benchmark.prediction import run_all_predictions

st.set_page_config(page_title="Prédiction", page_icon="🎯", layout="wide")
st.title("Prédiction downstream")
st.markdown("Les embeddings capturent-ils de l'information sémantique ? Testons-les comme features de classifieurs.")

if not has_results():
    st.error("Aucun résultat.")
    st.stop()

df = load_embeddings()
if df.empty:
    st.stop()

X = get_embedding_matrix(df)
station_ids = df["station_id"].tolist()
domains = df["domain"].tolist()

# Run predictions (cached)
@st.cache_data
def cached_predictions():
    return run_all_predictions(X, station_ids, domains, df)

with st.spinner("Entraînement des classifieurs..."):
    results = cached_predictions()

if not results:
    st.warning("Aucune tâche de prédiction possible avec les données disponibles.")
    st.stop()

# Display results per task
for task_result in results:
    task_name = task_result["task"]
    st.subheader(f"Tâche : prédire `{task_name}`")

    if "error" in task_result:
        st.warning(task_result["error"])
        continue

    st.write(f"**{task_result['n_samples']}** échantillons, **{task_result['n_classes']}** classes")

    # Metrics table
    metrics_data = []
    for clf_name in ["random_forest", "logistic_regression"]:
        if clf_name in task_result:
            m = task_result[clf_name]
            metrics_data.append({
                "Classifieur": clf_name.replace("_", " ").title(),
                "Accuracy": f"{m['accuracy']:.1%}",
                "F1 (weighted)": f"{m['f1_weighted']:.1%}",
            })
    st.table(pd.DataFrame(metrics_data))

    # Confusion matrix (RF)
    if "random_forest" in task_result:
        cm = np.array(task_result["random_forest"]["confusion_matrix"])
        classes = task_result.get("classes", [str(i) for i in range(len(cm))])

        # Truncate to top 15 classes for readability
        if len(classes) > 15:
            top_idx = np.argsort(cm.sum(axis=1))[-15:]
            cm = cm[np.ix_(top_idx, top_idx)]
            classes = [classes[i] for i in top_idx]

        fig_cm = px.imshow(cm, x=classes, y=classes, color_continuous_scale="Blues",
                           title=f"Matrice de confusion — Random Forest ({task_name})",
                           labels=dict(x="Prédit", y="Réel", color="Count"))
        st.plotly_chart(fig_cm, use_container_width=True)

    # Feature importance
    if "feature_importance" in task_result:
        imp = np.array(task_result["feature_importance"])
        top_n = 20
        top_idx = np.argsort(imp)[-top_n:]
        fig_imp = px.bar(x=imp[top_idx], y=[f"dim_{i}" for i in top_idx],
                         orientation="h", title=f"Top {top_n} dimensions (importance RF)",
                         labels=dict(x="Importance", y="Dimension"))
        fig_imp.update_layout(height=400)
        st.plotly_chart(fig_imp, use_container_width=True)
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/app/pages/4_prediction.py
git commit -m "feat(benchmark): page 4 — downstream prediction (domain, nature_eh, region)"
```

---

### Task 14: Page 5 — Temporal Analysis

**Files:**
- Create: `benchmark/app/pages/5_temporal_analysis.py`

- [ ] **Step 1: Write temporal analysis page**

```python
"""Page 5 : Analyse temporelle des embeddings."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
from scipy.spatial.distance import cosine
from embedding_benchmark.ui.components import (
    load_embeddings, load_window_embeddings, get_embedding_columns, has_results,
)

st.set_page_config(page_title="Analyse temporelle", page_icon="📈", layout="wide")
st.title("Analyse temporelle des embeddings")

if not has_results():
    st.error("Aucun résultat.")
    st.stop()

df_stations = load_embeddings()
df_windows = load_window_embeddings()

if df_windows.empty:
    st.warning("Pas de window embeddings. Relancez le benchmark.")
    st.stop()

emb_cols = get_embedding_columns(df_windows)

# ── Drift Leaderboard ──────────────────────────────────────────────────
st.subheader("Stations avec le plus de dérive temporelle")

# Compute drift per station
drift_scores = []
for sid, group in df_windows.groupby("station_id"):
    group = group.sort_values("window_idx")
    windows = group[emb_cols].values
    if len(windows) < 2:
        continue
    drifts = [cosine(windows[i], windows[i + 1]) for i in range(len(windows) - 1)]
    domain = group["domain"].iloc[0]
    drift_scores.append({
        "station_id": sid,
        "domain": domain,
        "max_drift": round(max(drifts), 4),
        "mean_drift": round(np.mean(drifts), 4),
        "n_windows": len(windows),
    })

drift_df = pd.DataFrame(drift_scores).sort_values("max_drift", ascending=False)

# Display top 20
st.dataframe(drift_df.head(20), use_container_width=True, hide_index=True)

fig_drift = px.bar(drift_df.head(20), x="station_id", y="max_drift", color="domain",
                   title="Top 20 stations — dérive maximale entre fenêtres consécutives")
fig_drift.update_layout(xaxis_tickangle=45)
st.plotly_chart(fig_drift, use_container_width=True)

# ── Station detail ─────────────────────────────────────────────────────
st.subheader("Évolution temporelle d'une station")

station_options = [f"[{row['domain']}] {row['station_id']}" for _, row in df_stations.iterrows()
                   if row["station_id"] in df_windows["station_id"].values]
if not station_options:
    st.stop()

selected = st.selectbox("Station", station_options)
selected_id = selected.split("] ")[1]

# Get windows for this station
station_windows = df_windows[df_windows["station_id"] == selected_id].sort_values("window_idx")
windows_emb = station_windows[emb_cols].values

if len(windows_emb) < 2:
    st.info("Pas assez de fenêtres pour cette station.")
    st.stop()

# Drift over time
drifts = [cosine(windows_emb[i], windows_emb[i + 1]) for i in range(len(windows_emb) - 1)]

fig_drift_ts = go.Figure()
fig_drift_ts.add_trace(go.Scatter(
    x=list(range(1, len(drifts) + 1)),
    y=drifts, mode="lines+markers", name="Drift (distance cosine)",
    line=dict(color="coral", width=2),
))
fig_drift_ts.update_layout(
    title=f"Dérive temporelle — {selected_id}",
    xaxis_title="Transition (fenêtre i → i+1)",
    yaxis_title="Distance cosine", height=350,
)
st.plotly_chart(fig_drift_ts, use_container_width=True)

# UMAP of this station's windows
if len(windows_emb) >= 3:
    import umap as umap_lib
    reducer = umap_lib.UMAP(n_components=2, n_neighbors=min(5, len(windows_emb) - 1),
                            random_state=42, metric="cosine")
    coords = reducer.fit_transform(windows_emb)

    fig_umap = px.scatter(
        x=coords[:, 0], y=coords[:, 1],
        color=station_windows["window_idx"].values.astype(str),
        title=f"UMAP des fenêtres — {selected_id}",
        labels=dict(x="UMAP 1", y="UMAP 2", color="Fenêtre"),
        width=700, height=500,
    )
    fig_umap.update_traces(marker=dict(size=10))
    st.plotly_chart(fig_umap, use_container_width=True)

# ── Timeline slider ────────────────────────────────────────────────────
st.subheader("UMAP global par fenêtre temporelle")

max_window = int(df_windows["window_idx"].max())
window_idx = st.slider("Index de fenêtre", 0, max_window, 0)

window_slice = df_windows[df_windows["window_idx"] == window_idx]
if len(window_slice) < 10:
    st.info(f"Seulement {len(window_slice)} stations ont une fenêtre #{window_idx}.")
else:
    X_slice = window_slice[emb_cols].values
    import umap as umap_lib
    reducer = umap_lib.UMAP(n_components=2, random_state=42, metric="cosine")
    coords = reducer.fit_transform(X_slice)

    fig_global = px.scatter(
        x=coords[:, 0], y=coords[:, 1],
        color=window_slice["domain"].values,
        hover_data=[window_slice["station_id"].values],
        title=f"UMAP de toutes les stations — fenêtre #{window_idx}",
        width=800, height=600,
    )
    fig_global.update_traces(marker=dict(size=5, opacity=0.7))
    st.plotly_chart(fig_global, use_container_width=True)
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/app/pages/5_temporal_analysis.py
git commit -m "feat(benchmark): page 5 — temporal analysis (drift, UMAP windows)"
```

---

## Chunk 4: Integration Test

### Task 15: Smoke test on small sample

- [ ] **Step 1: Run on 30+30 stations to verify everything works end-to-end**

```bash
cd benchmark && .venv/bin/python scripts/run_softclt.py --piezo 30 --hydro 30 --epochs 10
```

Expected: completes without error, prints metrics summary, creates files in `results/`.

- [ ] **Step 2: Verify files exist**

```bash
ls -la benchmark/results/embeddings/SoftCLT_unified.parquet
ls -la benchmark/results/windows/SoftCLT_unified_windows.parquet
ls -la benchmark/results/metrics/SoftCLT_unified.json
```

- [ ] **Step 3: Launch Streamlit and verify all 5 pages load**

```bash
cd benchmark && .venv/bin/python -m streamlit run app/app.py --server.headless true &
sleep 5
curl -s http://localhost:8501 | head -5
kill %1
```

- [ ] **Step 4: Fix any issues found**

- [ ] **Step 5: Commit all fixes**

```bash
git add -A benchmark/
git commit -m "fix(benchmark): smoke test fixes for unified SoftCLT platform"
```

---

### Task 16: Full run on 1k+1k stations

- [ ] **Step 1: Run full benchmark**

```bash
cd benchmark && .venv/bin/python scripts/run_softclt.py --piezo 1000 --hydro 1000 --epochs 100
```

Expected: completes in 30-60 min, produces results for ~2k stations.

- [ ] **Step 2: Commit results metadata (not data files)**

Results are gitignored. Just verify the pipeline completed successfully and record the metrics.

- [ ] **Step 3: Final commit**

```bash
git add -A benchmark/
git commit -m "feat(benchmark): SoftCLT embedding platform — piezo + hydro, 5 demo pages"
```
