# SoftCLT Warehouse Integration — Design Spec

> **Date**: 2026-03-12
> **Status**: Draft
> **Method**: SoftCLT (TS2Vec + hierarchical soft contrastive loss)
> **Scope**: Full CDC — 2 domain models, 4 `ml.*` tables, nightly sensor-driven encoding
> **Predecessor**: `docs/plans/2026-03-10-latent-space-piezometry.md` (TS2Vec CDC, superseded)

---

## 1. Context & Motivation

The embedding benchmark (30-station quick eval + 1k+1k full run) validated **SoftCLT** as the best method:
- Silhouette score: **0.69** (vs 0.41 tsfresh, 0.52 TS2Vec)
- Temporal stability: **99%** kNN coherence
- Training time: ~15min CPU for 2000 stations (acceptable without GPU)

SoftCLT is TS2Vec with a monkey-patched `hierarchical_contrastive_loss` that uses soft assignments instead of hard positives/negatives. The code is vendorized from the benchmark (`benchmark/src/embedding_benchmark/methods/softclt_method.py`).

This spec integrates SoftCLT embeddings into the production data warehouse as first-class assets, enabling downstream applications (similarity search, clustering, anomaly detection, forecasting covariates) via pgvector.

---

## 2. Architecture

### 2.1 Two Domain-Specific Models

Separate encoders for different physical dynamics:

| Model | Input Variables | Stations | Rationale |
|-------|----------------|----------|-----------|
| **Piézo+ERA5** | `niveau_nappe_eau`, `temperature_2m`, `total_precipitation`, `potential_evaporation` | ~2,935 | Nappe inertia: slow response (weeks-months), annual cycle |
| **Hydro+ERA5** | `resultat_obs_elab` (QmnJ), `temperature_2m`, `total_precipitation`, `potential_evaporation` | ~2,535 | River response: fast (hours-days), event-driven |

Per-domain normalization: separate `StandardScaler` per domain (meters vs l/s scales).

### 2.2 Hyperparameters

#### Architecture
| Param | Value | Rationale |
|-------|-------|-----------|
| `input_dims` | 4 | 1 hydro variable + 3 ERA5 climate variables |
| `hidden_dims` | **64** | TS2Vec/SoftCLT paper default. Internal CNN filter count |
| `output_dims` | 320 | Embedding dimension. TS2Vec paper default |
| `depth` | 10 | 10 dilated conv layers → receptive field = 1024 timesteps (~2.8 years) |

#### Training
| Param | Value | Rationale |
|-------|-------|-----------|
| `batch_size` | **128** | Maximizes GPU utilization on A6000 48GB with hidden=64 |
| `n_epochs` | 200 | Max epochs (early stopping may terminate sooner) |
| `lr` | 1e-3 | AdamW default, standard for TS2Vec |
| `max_train_length` | **1500** | Cap for padding/splitting long series. >1024 (receptive field), limits memory |
| `early_stop_patience` | **20** | Stop if loss doesn't improve for 20 consecutive epochs |

#### Encoding
| Param | Value | Rationale |
|-------|-------|-----------|
| `window_size` | 365 | 1 hydrological cycle — captures full seasonal pattern |
| `stride` | 90 | 75% overlap, ~4 windows/year. Balances resolution vs volume |
| `station_embedding` | mean pooling | Average of all window embeddings → stable station-level representation |

#### Eligibility
- ≥730 days of data (2 full cycles minimum for meaningful windows)
- Last measurement ≥2024-01-01 (exclude defunct stations)

### 2.3 Preprocessing & Normalization Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│ TRAINING (manual, ~33 min/domain on RTX A6000)                 │
│                                                                 │
│ 1. SQL: SELECT from gold.{domain}_daily_chroniques              │
│    WHERE station eligible (≥730 days, active 2024+)             │
│    → {station_id: (T_i, 4)} dict, variable-length              │
│                                                                 │
│ 2. _interpolate_and_fill():                                     │
│    - Linear interpolation for interior NaN gaps (per column)    │
│    - Remaining NaN (edge cases) → 0                             │
│                                                                 │
│ 3. StandardScaler (GLOBAL per-feature):                         │
│    all_data = concat([arr for arr in series_dict.values()])     │
│    scaler.fit(all_data)  → mean/std per column                  │
│    scaled = [scaler.transform(arr) for arr in series]           │
│    ⚠ Global, NOT per-station — preserves absolute scale         │
│                                                                 │
│ 4. TS2Vec padding + splitting:                                  │
│    - Pad all series to min(max_len, max_train_length=1500)      │
│    - Series > 1500: split into chunks, each ≤1500               │
│    - NaN padding for variable-length centering                  │
│                                                                 │
│ 5. SoftCLT contrastive training:                                │
│    - Random crop augmentation (2 views per series)              │
│    - Soft instance CL: cosine similarity soft labels            │
│    - Soft temporal CL: sigmoid timelag weighting                │
│    - Hierarchical: max-pool 2x at each scale level              │
│                                                                 │
│ 6. Save: model.pt + scaler.pkl + stations.json                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ NIGHTLY ENCODING (sensor-driven, ~5 min/domain on GPU)         │
│                                                                 │
│ 1. Load model.pt + scaler.pkl (SAME scaler as training)        │
│                                                                 │
│ 2. For each station:                                            │
│    a. scaler.transform(raw_arr)  → normalized with train stats  │
│    b. Sliding windows: 365d window, 90d stride                  │
│    c. encode(window) → (320,) embedding per window              │
│    d. mean(window_embeddings) → (320,) station embedding        │
│                                                                 │
│ 3. Upsert to pgvector:                                         │
│    - ml.{domain}_station_embeddings (HNSW cosine index)        │
│    - ml.{domain}_window_embeddings                             │
│                                                                 │
│ 4. HDBSCAN clustering → update cluster_id                      │
└─────────────────────────────────────────────────────────────────┘
```

#### Why Global Normalization (not per-station)?

**Global StandardScaler** (fit on all stations concatenated, per-feature) is the correct choice:

- **Per-station z-score** would make all stations scale-invariant → a deep aquifer at -50m and a shallow one at +2m would have identical normalized profiles. This loses physically meaningful information.
- **Global normalization** preserves relative magnitudes across stations. Two stations with similar levels AND similar dynamics will be close in embedding space.
- This follows the **TS2Vec paper** (Yue et al., AAAI 2022) which normalizes per-dataset, not per-instance.
- The 4 features have very different scales (meters, Kelvin, mm) → per-feature normalization is necessary. `StandardScaler` does this by default.

### 2.3 Volumetry

| | Piézo | Hydro | Total |
|--|-------|-------|-------|
| Stations | 2,935 | 2,535 | 5,470 |
| Windows (365d/90d stride) | ~208K | ~161K | ~369K |
| Storage (vector(320) + metadata + HNSW) | ~255 MB | ~195 MB | ~450 MB |

Negligible on a 76 GB database.

### 2.4 Pipeline Integration

```
Bronze → shared_staging → piezo_daily ──→ dimensions
                                     ╰──→ ml_piezo_encode → ml_piezo_cluster  [NEW]
                        → hydro_daily ──→ dimensions
                                     ╰──→ ml_hydro_encode → ml_hydro_cluster  [NEW]
```

Embedding jobs run **in parallel** with the existing dimensions sensor. No dependency between them.

### 2.5 Training vs Encoding

| Operation | Trigger | Duration (GPU) | Frequency |
|-----------|---------|----------------|-----------|
| Training | Manual (Dagster UI) | ~33 min | Monthly or on-demand |
| Nightly encoding | Sensor (after domain pipeline) | ~5 min | Daily |
| Clustering | After encoding | ~30s (CPU) | Daily |

Training produces `model.pt` + `scaler.pkl` + `stations.json`. Nightly encoding loads the latest model and re-encodes all eligible stations (v1: full re-encode; v2: incremental).

**Monitoring**: Training logs epoch progress (loss, best, ETA) to Dagster structured events every 10 epochs. Early stopping halts training after 20 epochs without improvement.

---

## 3. PostgreSQL Schema

### 3.1 Extensions & Schema

Added to `docker/postgres/init.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS ml;
```

pgvector 0.8.1 is already available in `timescale/timescaledb-ha:pg16`.

### 3.2 Tables

```sql
-- Piézométrie
CREATE TABLE ml.piezo_station_embeddings (
    code_bss        TEXT PRIMARY KEY,
    embedding       vector(320) NOT NULL,
    cluster_id      INT,
    model_version   TEXT NOT NULL,
    n_days          INT NOT NULL,
    n_windows       INT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE ml.piezo_window_embeddings (
    code_bss        TEXT NOT NULL,
    window_start    DATE NOT NULL,
    window_end      DATE NOT NULL,
    embedding       vector(320) NOT NULL,
    model_version   TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code_bss, window_start)
);

-- Hydrométrie
CREATE TABLE ml.hydro_station_embeddings (
    code_station    TEXT PRIMARY KEY,
    embedding       vector(320) NOT NULL,
    cluster_id      INT,
    model_version   TEXT NOT NULL,
    n_days          INT NOT NULL,
    n_windows       INT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE ml.hydro_window_embeddings (
    code_station    TEXT NOT NULL,
    window_start    DATE NOT NULL,
    window_end      DATE NOT NULL,
    embedding       vector(320) NOT NULL,
    model_version   TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code_station, window_start)
);
```

### 3.3 Indexes

```sql
-- HNSW for similarity search on station embeddings (cosine distance)
CREATE INDEX idx_piezo_station_emb_hnsw
    ON ml.piezo_station_embeddings
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

CREATE INDEX idx_hydro_station_emb_hnsw
    ON ml.hydro_station_embeddings
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

-- B-tree for window lookup by station
CREATE INDEX idx_piezo_window_station ON ml.piezo_window_embeddings (code_bss, window_start);
CREATE INDEX idx_hydro_window_station ON ml.hydro_window_embeddings (code_station, window_start);
```

HNSW chosen over IVFFlat: better recall, no VACUUM needed, optimal for <1M vectors.

---

## 4. File Structure

```
src/hubeau_pipeline/
├── ml/
│   ├── __init__.py
│   ├── ts2vec/                       # Vendorized TS2Vec core
│   │   ├── __init__.py
│   │   ├── ts2vec.py                 # Main TS2Vec class
│   │   ├── encoder.py                # Dilated CNN backbone
│   │   ├── losses.py                 # Original + SoftCLT loss
│   │   └── utils.py                  # Padding, split utilities
│   └── latent_space/
│       ├── __init__.py
│       ├── encoder.py                # SoftCLTEncoder wrapper (fit/encode/save/load)
│       ├── data.py                   # Load series from Gold tables
│       ├── persistence.py            # pgvector CRUD (upsert, search, init schema)
│       └── clustering.py             # HDBSCAN + metrics
├── assets/
│   └── ml_assets.py                  # 6 Dagster assets (2 train + 2 encode + 2 cluster)
├── jobs/
│   └── ml_jobs.py                    # 4 jobs (2 train + 2 nightly)
```

### Key Difference from TS2Vec Plan

The `losses.py` file contains the **SoftCLT monkey-patch**: `soft_contrastive_loss()` replaces `hierarchical_contrastive_loss()` in TS2Vec. This is the only code change vs vanilla TS2Vec — the encoder architecture, training loop, and inference are identical.

---

## 5. Components

### 5.1 SoftCLTEncoder (encoder.py)

Wraps TS2Vec with SoftCLT loss monkey-patch and per-domain normalization:

- `__init__(input_dims, embedding_dim, hidden_dim, depth, device)`
- `fit(train_series, n_epochs, lr, batch_size)` — applies SoftCLT patch before training
- `encode_windows(series, window_size, stride, dates)` → `(n_windows, 320)` + date ranges
- `station_embedding(window_embeddings)` → mean pooling → `(320,)`
- `save(path)` / `load(path)` — model.pt + scaler.pkl

### 5.2 Data Loading (data.py)

Reuses the same SQL queries as the benchmark `data_loader.py`:
- `load_piezo_series(pg, min_days=730)` → `{code_bss: (T, 4)}`
- `load_hydro_series(pg, min_days=730)` → `{code_station: (T, 4)}`
- `load_piezo_dates(pg, min_days=730)` → `{code_bss: [dates]}`
- `load_hydro_dates(pg, min_days=730)` → `{code_station: [dates]}`

Uses `PostgreSQLResource.get_connection()` instead of raw `psycopg2.connect()`.

### 5.3 Persistence (persistence.py)

pgvector CRUD operations:
- `init_ml_schema(pg)` — CREATE EXTENSION, SCHEMA, TABLES, INDEXES (idempotent)
- `upsert_station_embeddings(pg, table, id_col, embeddings, version)` — ON CONFLICT DO UPDATE
- `upsert_window_embeddings(pg, table, id_col, embeddings, version)` — ON CONFLICT DO UPDATE
- `search_similar(pg, table, id_col, station_id, k)` — cosine kNN via HNSW

Generalized (not duplicated per domain) — table/id_col params handle piezo vs hydro.

### 5.4 Clustering (clustering.py)

- `cluster_and_update(pg, table, id_col, min_cluster_size=5)` → HDBSCAN + UPDATE cluster_id
- Returns `{n_clusters, n_noise, silhouette_score, davies_bouldin_index}`

---

## 6. Dagster Assets

6 assets in 2 groups:

| Asset | Group | Trigger | Dependencies |
|-------|-------|---------|--------------|
| `ml_piezo_model_train` | ml_piezo | Manual | `hubeau_daily_chroniques` |
| `ml_piezo_embeddings_update` | ml_piezo | Sensor | `hubeau_daily_chroniques` |
| `ml_piezo_clusters` | ml_piezo | After encode | `ml_piezo_embeddings_update` |
| `ml_hydro_model_train` | ml_hydro | Manual | `hydro_daily_chroniques` |
| `ml_hydro_embeddings_update` | ml_hydro | Sensor | `hydro_daily_chroniques` |
| `ml_hydro_clusters` | ml_hydro | After encode | `ml_hydro_embeddings_update` |

---

## 7. Jobs

4 jobs:

| Job | Assets | Usage |
|-----|--------|-------|
| `ml_piezo_train_job` | `ml_piezo_model_train` | Manual via Dagster UI |
| `ml_hydro_train_job` | `ml_hydro_model_train` | Manual via Dagster UI |
| `ml_piezo_embeddings_job` | `ml_piezo_embeddings_update` + `ml_piezo_clusters` | Nightly (sensor) |
| `ml_hydro_embeddings_job` | `ml_hydro_embeddings_update` + `ml_hydro_clusters` | Nightly (sensor) |

---

## 8. Sensor

New `domain_to_embeddings_sensor` added to `sensors.py`:

- Type: `run_status_sensor`
- Monitors: `dbt_piezo_pipeline_daily_job`, `dbt_hydro_pipeline_daily_job`
- On piezo success → `ml_piezo_embeddings_job`
- On hydro success → `ml_hydro_embeddings_job`
- Runs in parallel with existing `domain_to_dimensions_sensor`

### Nightly Chain

```
3h00  ERA5 Smart Update
4h00  Bronze piézo + hydro
      │
      ▼ (sensor step 1)
5h00  Shared staging (ERA5)
      │
      ▼ (sensor step 2)
5h30  ┌─ Piezo domain pipeline ──→ Piezo embeddings + clusters  [NEW]
      └─ Hydro domain pipeline ──→ Hydro embeddings + clusters  [NEW]
      │
      ▼ (sensor step 3, after both domains)
7h00  Shared dimensions
```

---

## 9. Docker & Dependencies

### 9.1 Worker Dockerfile

Add to `docker/worker/Dockerfile` (or `pyproject.toml` deps):
- `torch` (CPU-only via `--index-url https://download.pytorch.org/whl/cpu`)
- `pgvector>=0.3.0`
- `hdbscan>=0.8.33`
- `scikit-learn>=1.3.0` (already present)
- `joblib>=1.3.0` (already present)

### 9.2 GPU Support

GPU is enabled via NVIDIA Container Toolkit (v1.18.2):
```yaml
# docker-compose.yml - dlt_worker
deploy:
  resources:
    reservations:
      devices:
        - driver: nvidia
          count: 1
          capabilities: [gpu]
environment:
  NVIDIA_VISIBLE_DEVICES: all
mem_limit: 32G  # Training peaks at ~17GB RAM (padding + tensors)
```

**Measured performance** (RTX A6000, 48GB VRAM):
| Operation | Duration | GPU util | VRAM |
|-----------|----------|----------|------|
| Piezo training (2,936 stations, 200 epochs) | **33 min** | 80-100% | 32-48 GB |
| Hydro training (2,535 stations, 200 epochs) | ~28 min | 80-100% | 32-48 GB |
| Nightly encoding (~3,000 stations) | ~5 min | 30-50% | ~2 GB |

CPU fallback works via `device="auto"` — ~10x slower for training.

### 9.3 Model Persistence

Models stored in external Docker volume `brgm_ml_models` mounted at `/var/ml/models`:
- `piezo_latest` → symlink to latest model version directory
- `hydro_latest` → symlink to latest model version directory
- Each version dir contains: `model.pt`, `scaler.pkl`, `stations.json`

Add to `scripts/init_volumes.sh`:
```bash
docker volume create brgm_ml_models
```

### 9.4 init.sql

Add to `docker/postgres/init.sql`:
```sql
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS ml;
```

---

## 10. Wiring (definitions.py)

```python
# New imports
from .assets.ml_assets import (
    ml_piezo_model_train, ml_hydro_model_train,
    ml_piezo_embeddings_update, ml_hydro_embeddings_update,
    ml_piezo_clusters, ml_hydro_clusters,
)
from .jobs.ml_jobs import (
    ml_piezo_train_job, ml_hydro_train_job,
    ml_piezo_embeddings_job, ml_hydro_embeddings_job,
)
from .sensors import domain_to_embeddings_sensor

# Add to existing Definitions()
assets=[*all_assets, ml_piezo_model_train, ml_hydro_model_train,
        ml_piezo_embeddings_update, ml_hydro_embeddings_update,
        ml_piezo_clusters, ml_hydro_clusters]
jobs=[*all_jobs, ml_piezo_train_job, ml_hydro_train_job,
      ml_piezo_embeddings_job, ml_hydro_embeddings_job]
sensors=[*all_sensors, domain_to_embeddings_sensor]
```

---

## 11. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| No GPU on server | `device="auto"` CPU fallback (~15min nightly) |
| OOM loading 23M rows | SQL subquery filters eligibility server-side |
| PyTorch image size (+2GB) | CPU-only wheel (~200MB) |
| Model lost on container restart | External volume `brgm_ml_models` |
| HDBSCAN unstable in dim 320 | Pre-reduce with UMAP(50) if silhouette < 0.3 |
| `max_concurrent_runs: 1` blocks GPU job | Embedding job ~5min, queue wait acceptable |
| Nightly full re-encode (not incremental) | v2: track modified stations, skip unchanged |

---

## 12. Success Metrics

| Metric | Target |
|--------|--------|
| Silhouette score (HDBSCAN) | > 0.5 |
| Temporal stability (kNN coherence) | > 95% |
| Nightly encoding time (CPU) | < 15 min |
| pgvector kNN query time (HNSW) | < 50 ms |
| Total `ml.*` storage | < 600 MB |
| Cluster homogeneity (nature_eh / type_site) | > 70% |

---

## 13. Out of Scope (Future Work)

- **Cross-modal model**: 3rd SoftCLT model for nappe-river exchange detection (CLIP-style contrastive or latent correlation). Requires station pairing by proximity.
- **time-serie-explo API**: REST endpoints for similarity search, clustering, anomaly detection. Separate project using the `ml.*` tables as data source.
- **Incremental encoding**: Only re-encode stations with new data since last run.
- **Forecasting covariates**: Use station embeddings as static features in time series models.
