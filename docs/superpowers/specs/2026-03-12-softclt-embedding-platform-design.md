# SoftCLT Embedding Platform — Piezo + Hydro

**Date**: 2026-03-12
**Status**: Approved
**Scope**: Expand benchmark to 2k stations (1k piezo + 1k hydro), unified SoftCLT model, 5-page Streamlit demo UI

## Context

The benchmark project (`benchmark/`) validated SoftCLT as the best embedding method for hydrological time series (silhouette 0.69, 99% temporal stability, 93% cluster assignment on 30 stations). This spec expands it to a full demo platform.

## Architecture

### Single Unified Model

One SoftCLT model trained on ~2000 stations (1k piezo + 1k hydro). This enables cross-domain similarity discovery (e.g., a piezometer that behaves like a river).

### Data Sources

| Domain | Gold Table | Measurement Column | Unit | Filter | Stratify By |
|--------|-----------|-------------------|------|--------|-------------|
| Piezo | `gold.hubeau_daily_chroniques` | `niveau_nappe_eau` | meters | ≥730 days, last read ≥2024-01-01 | `nature_eh` |
| Hydro | `gold.hydro_daily_chroniques` | `resultat_obs_elab` | l/s | `grandeur_hydro_elab = 'QmnJ'`, ≥730 days, last read ≥2024-01-01 | `type_site` |

Both include 3 ERA5 variables: `temperature_2m`, `total_precipitation`, `potential_evaporation`.

### Unified Station ID Strategy

All code uses a generic `station_id` column (not `code_bss`), plus a `domain` column:

| Domain | station_id source | domain value |
|--------|------------------|-------------|
| Piezo | `code_bss` | `"piezo"` |
| Hydro | `code_station` | `"hydro"` |

Every file referencing `code_bss` must be refactored: `data_loader.py`, `evaluation.py`, `methods/__init__.py` (MethodResult), `ui/components.py`, all Streamlit pages. The `MethodResult.station_ids` field becomes a list of generic station IDs.

### Hydro Eligibility Query

```sql
SELECT code_station, type_site, code_departement,
       COUNT(*) as n_days, MAX(date_obs) as last_date
FROM gold.hydro_daily_chroniques
WHERE grandeur_hydro_elab = 'QmnJ'
GROUP BY code_station, type_site, code_departement
HAVING COUNT(*) >= 730 AND MAX(date_obs) >= '2024-01-01'
```

If fewer than 1000 eligible hydro stations exist, use all available (minimum threshold: 200).

### Sampling

- Piezo: 1000 stations, stratified by `nature_eh`
- Hydro: 1000 stations (or all if <1000), stratified by `type_site`
- Seed: 42 (reproducible)

### Normalization

Two StandardScalers (one per domain), fit on training data. Process:
1. Scale piezo measurement + 3 ERA5 with piezo scaler
2. Scale hydro measurement + 3 ERA5 with hydro scaler
3. Concatenate scaled arrays → single training dataset for SoftCLT

The first variable dimension represents different physical quantities per domain (water level vs discharge), but after scaling both are zero-mean unit-variance, which is what the contrastive loss needs.

### Windowing

- Window size: 365 days
- Stride: 90 days (quarterly)
- ~20 windows per station (5 years of data)

## SoftCLT Training

- Input: ~2000 stations x ~20 windows x 365 days x 4 variables
- Embedding dim: 320
- Epochs: 100 (reduced from 200 for scaling)
- Batch size: 32 (increased from 16)
- Learning rate: 1e-3
- Output: station-level (2000, 320) + window-level (~40000, 320)
- Estimated time: 30-60 min on CPU (conservative)
- Model serialized via `torch.save()` to `results/models/softclt_unified.pt`

## Evaluation Metrics

Domain-aware evaluation:

| Metric | Piezo | Hydro | Unified |
|--------|-------|-------|---------|
| Silhouette | per-domain | per-domain | global |
| ARI | vs `nature_eh` | vs `type_site` | — |
| kNN coherence | vs `nature_eh` | vs `type_site` | vs `domain` |
| Temporal stability | per-domain | per-domain | global |

## Streamlit UI — 5 Pages

### Page 1: Clustering & Exploration

- UMAP 2D projection (Plotly scatter)
- Color by: HDBSCAN cluster, domain (piezo/hydro), nature_eh (piezo) / type_site (hydro), department
- Sidebar filters: domain, cluster ID
- Stats panel: n_clusters, distribution table, silhouette score
- HDBSCAN parameters adjustable (min_cluster_size, min_samples)

### Page 2: Similarity Search (kNN)

- Dropdown to select a station (searchable, shows domain prefix)
- Slider for K neighbors (1-50)
- Results table: rank, station_id, domain, distance, metadata
- Side-by-side time series plot: query station vs top-K neighbors (normalized)
- Cross-domain highlight when piezo matches hydro or vice versa

### Page 3: Anomaly Detection

- Two methods: Isolation Forest + Local Outlier Factor (LOF)
- Contamination parameter adjustable (slider 1-20%)
- UMAP with anomalies highlighted (red markers)
- Anomaly table: station_id, domain, anomaly score, nearest normal neighbor
- Comparison plot: anomalous station time series vs nearest "normal" station

### Page 4: Downstream Prediction

- Embeddings as features for classifiers (Random Forest, Logistic Regression)
- 3 prediction tasks:
  - Predict `nature_eh` (piezo only, ~10 classes)
  - Predict `domain` (piezo vs hydro, binary)
  - Predict region (multi-class, ~13 classes — not department which has ~90 classes with too few samples)
- Display: accuracy, F1-score, confusion matrix (Plotly heatmap)
- Feature importance: which embedding dimensions matter most (bar chart)
- Train/test split: 80/20, stratified

### Page 5: Temporal Analysis

- Select a station → show window embeddings evolution over time
- UMAP of all windows for selected station, colored by date
- Drift score: cosine distance between consecutive windows
- "Most drifting stations" leaderboard (top-20 by max drift)
- Timeline slider: UMAP of all stations at a specific window index

## File Structure

```
benchmark/
├── src/embedding_benchmark/
│   ├── config.py                    # Extended: hydro_cols, unified params
│   ├── data_loader.py               # Refactored: station_id, domain, piezo+hydro loaders
│   ├── methods/
│   │   ├── __init__.py              # MethodResult: station_ids (generic)
│   │   └── softclt_method.py        # Accept domain-mixed data
│   ├── evaluation.py                # Refactored: domain-aware metrics, station_id
│   ├── anomaly.py                   # NEW: IsolationForest + LOF
│   ├── prediction.py                # NEW: downstream classifiers
│   └── ui/
│       └── components.py            # Refactored: station_id, load unified data
├── app/
│   ├── app.py                       # Updated: 5 pages navigation
│   └── pages/
│       ├── 1_clustering.py          # Rewritten
│       ├── 2_similarity.py          # Rewritten
│       ├── 3_anomaly_detection.py   # NEW
│       ├── 4_prediction.py          # NEW
│       └── 5_temporal_analysis.py   # NEW
├── scripts/
│   ├── run_all.py                   # Kept for legacy 5-method comparison
│   └── run_softclt.py               # NEW: unified piezo+hydro SoftCLT run
└── results/
    ├── embeddings/
    │   └── SoftCLT_unified.parquet  # (2000, 320 + station_id + domain + metadata)
    ├── windows/
    │   └── SoftCLT_windows.parquet  # (~40000, 320 + station_id + window_idx + dates)
    ├── metrics/
    │   └── SoftCLT_unified.json
    └── models/
        └── softclt_unified.pt       # torch.save() format
```

New function needed: `save_window_embeddings()` in `evaluation.py` to serialize window-level embeddings to parquet.

## Dependencies

No new pip dependencies needed. Already available:
- `scikit-learn`: IsolationForest, LOF, RandomForest, LogisticRegression, HDBSCAN
- `plotly`: interactive charts
- `umap-learn`: dimensionality reduction
- `streamlit`: UI framework
- `torch`: model serialization

## Success Criteria

1. SoftCLT trains successfully on 2k stations in <60 min
2. HDBSCAN finds ≥5 meaningful clusters (noise <50%)
3. Cross-domain similarities visible (piezo-hydro neighbors in kNN)
4. Anomaly detection identifies ≥10 outlier stations
5. Downstream prediction: domain accuracy >90%, nature_eh accuracy >40%
6. All 5 Streamlit pages functional and responsive
