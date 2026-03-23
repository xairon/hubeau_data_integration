# ML Pipeline — Station Embeddings & Clustering

> **Date**: 2026-03-17
> **Status**: Production
> **Schema**: `ml.*` (PostgreSQL + pgvector)

---

## 1. Vue d'ensemble

Le pipeline ML calcule des **embeddings vectoriels** pour chaque station hydrologique (piézomètres et stations hydrométriques). Ces embeddings encodent le comportement temporel des stations et permettent :

- **Recherche de similarité** : trouver les stations au comportement le plus proche (kNN via pgvector HNSW)
- **Clustering** : regrouper les stations par type de dynamique (HDBSCAN)
- **Anomaly detection** : identifier les stations atypiques
- **Covariates statiques** : enrichir les modèles de prévision avec un vecteur station

### Architecture duale : 2 espaces d'embedding

Chaque station est encodée dans **deux espaces complémentaires** :

| Espace | Variables d'entrée | Encodeur | GPU | Ce qu'il capture |
|--------|-------------------|----------|-----|-----------------|
| **Multi** (4D) | Variable cible + `temperature_2m` + `total_precipitation` + `potential_evaporation` | SoftCLT | Oui | Dynamique hydro-climatique couplée |
| **Uni** (1D) | Variable cible seule (`niveau_nappe_eau` ou `resultat_obs_elab`) | MiniRocket+PCA | Non | Forme intrinsèque du signal |

Les deux espaces sont stockés dans les mêmes tables (colonne `space = 'multi'|'uni'`), chacun avec son propre clustering.

---

## 2. Encodeurs

### 2.1 SoftCLT — Espace multivarié

**Modèle** : TS2Vec (Yue et al., AAAI 2022) avec la loss SoftCLT (Seunghan, 2023) — apprentissage contrastif hiérarchique soft.

**Code** : `src/hubeau_pipeline/ml/latent_space/encoder.py` (`SoftCLTEncoder`)

**Principe** :
1. Les séries multivariate (T, 4) de chaque station sont découpées en fenêtres glissantes (365j, stride 90j)
2. TS2Vec encode chaque fenêtre en un vecteur de 320 dimensions via un réseau de convolutions dilatées
3. La loss SoftCLT remplace la loss contrastive dure de TS2Vec par des labels soft :
   - **Instance CL** : similarité cosine entre représentations moyennes → poids soft (pas binaire positif/négatif)
   - **Temporal CL** : pondération sigmoid basée sur le décalage temporel (timesteps proches = plus similaires)
   - **Hiérarchique** : max-pooling 2x à chaque échelle, la loss opère à toutes les résolutions
4. L'embedding station = mean pooling des embeddings de toutes ses fenêtres

**Hyperparamètres** :

| Paramètre | Valeur | Justification |
|-----------|--------|---------------|
| `input_dims` | 4 | 1 variable cible + 3 ERA5 |
| `embedding_dim` | 320 | Standard TS2Vec |
| `hidden_dim` | 64 | Capacité suffisante pour 4 vars |
| `depth` | 10 | Champ réceptif 2^10 = 1024j (~2.8 ans) |
| `window_size` | 365j | 1 cycle hydrologique complet |
| `stride` | 90j | ~4 fenêtres/an |
| `batch_size` | 128 | Optimisé pour A6000 48GB VRAM |
| `n_epochs` | 200 (max) | Early stopping patience=20 |
| `max_train_length` | 1500 | Cap padding pour éviter OOM |
| `lr` | 1e-3 | AdamW default |

**Vendorisation** :
- `src/hubeau_pipeline/ml/ts2vec/` — TS2Vec core (encoder, dilated CNN, utils)
- `src/hubeau_pipeline/ml/softclt/` — SoftCLT loss, timelags, hard losses
- Le monkey-patch `_patch_softclt_loss()` remplace la loss dans TS2Vec au runtime

**Performance mesurée (RTX A6000)** :

| Opération | Durée | VRAM |
|-----------|-------|------|
| Training piezo (~2 936 stations) | ~33 min | 32-48 GB |
| Training hydro (~2 535 stations) | ~28 min | 32-48 GB |
| Nightly encoding (~3 000 stations) | ~5 min | ~2 GB |

Fallback CPU via `device="auto"` (~10x plus lent pour le training).

### 2.2 MiniRocket+PCA — Espace univarié

**Modèle** : MiniRocket (Dempster et al., 2021, via `aeon`) + StandardScaler + PCA.

**Code** : `src/hubeau_pipeline/ml/latent_space/rocket_encoder.py` (`RocketEncoder`)

**Principe** :
1. Les séries univariate (T, 1) sont découpées en fenêtres glissantes (365j, stride 90j)
2. MiniRocket applique 9 996 noyaux convolutifs aléatoires (PPV — proportion of positive values) sur chaque fenêtre
3. Les 9 996 features sont normalisées (StandardScaler) puis réduites à 320 dimensions via PCA
4. L'embedding station = mean pooling des embeddings de toutes ses fenêtres

**Avantages** :
- **Pas de GPU** : MiniRocket est CPU-only, très rapide
- **Déterministe** : avec `random_state=42`, pas besoin de training itératif
- **Pas de backpropagation** : les noyaux sont aléatoires (pas entraînés)

**Pipeline** :
```
Fenêtres (365j) → MiniRocket (9996 features) → StandardScaler → PCA (320d) → mean pool
```

**Hyperparamètres** :

| Paramètre | Valeur |
|-----------|--------|
| `embedding_dim` | 320 |
| `window_size` | 365j |
| `stride` | 90j |
| `random_state` | 42 |
| Fit MiniRocket | sur 5 000 fenêtres (subset) |
| Fit PCA | sur 30 000 fenêtres (subset) |

**Batching** : le transform MiniRocket est fait par lots de 20 000 fenêtres pour éviter les OOM sur les grands datasets.

---

## 3. Preprocessing des données

### 3.1 Chargement depuis Gold

**Code** : `src/hubeau_pipeline/ml/latent_space/data.py`

4 loaders par domaine x espace :

| Loader | Domaine | Espace | Colonnes | Output |
|--------|---------|--------|----------|--------|
| `load_piezo_series()` | Piézo | Multi | `niveau_nappe_eau` + 3 ERA5 | `{code_bss: (T, 4)}` |
| `load_piezo_series_univariate()` | Piézo | Uni | `niveau_nappe_eau` | `{code_bss: (T, 1)}` |
| `load_hydro_series()` | Hydro | Multi | `resultat_obs_elab` + 3 ERA5 | `{code_station: (T, 4)}` |
| `load_hydro_series_univariate()` | Hydro | Uni | `resultat_obs_elab` | `{code_station: (T, 1)}` |

**Critères d'éligibilité** :
- **min_days = 540** (~1.5 ans) — garantit au moins 2 fenêtres de 365j
- Pas de filtre de récence — les stations inactives avec un long historique enrichissent l'apprentissage contrastif

### 3.2 Interpolation

`_interpolate_and_fill()` : interpolation linéaire par colonne pour les NaN internes, puis remplissage des NaN restants (bords) par 0.

### 3.3 Normalisation

**Multi (SoftCLT)** : `StandardScaler` **global** (fit sur toutes les stations concaténées, par feature). Pas de normalisation par station — préserve les magnitudes relatives (un aquifère profond à -50m et un superficiel à +2m restent distincts).

**Uni (MiniRocket)** : le scaler est intégré dans le `RocketEncoder` (fit sur les features MiniRocket, pas sur les séries brutes).

---

## 4. Clustering

### 4.1 Pipeline UMAP + HDBSCAN

**Code** : `src/hubeau_pipeline/ml/latent_space/clustering.py`

Chaque (domaine, espace) est clusterisé avec **deux configurations** :

| Config | But | UMAP dims | min_cluster_size | min_samples |
|--------|-----|-----------|-----------------|-------------|
| **Wide** (default) | Groupes larges, vue macro | 15 (multi) / 5 (uni) | 25 (multi) / 15 (uni) | 10 (multi) / 5 (uni) |
| **Fine** | Sous-groupes fins | 10 (multi) / 5 (uni) | 10 | 5 (multi) / 3 (uni) |

Pipeline : embeddings 320d → UMAP cosine → HDBSCAN euclidien → labels.

Les UMAP 2D/3D pour la visualisation sont aussi calculés et stockés.

### 4.2 Tuning (optionnel)

**Code** : `src/hubeau_pipeline/ml/latent_space/tuning.py`

Optuna TPE optimizer (80 trials, 300s timeout) qui maximise le score DBCV avec pénalité de bruit. Peut être activé via `tune=True` dans `cluster_and_update()`.

### 4.3 Métriques de qualité

- **DBCV** (relative_validity_) : métrique native HDBSCAN pour clusters non-convexes
- **Silhouette** : cohérence intra/inter-cluster (exclut le bruit label=-1)
- **Davies-Bouldin** : ratio de dispersion intra/séparation inter (plus bas = mieux)
- **Calinski-Harabasz** : ratio de variance inter/intra (plus haut = mieux)
- **Noise ratio** : proportion de stations non assignées

---

## 5. Stockage PostgreSQL

### 5.1 Schema `ml`

**Code** : `src/hubeau_pipeline/ml/latent_space/persistence.py`

Extensions : `pgvector` (installé dans `timescale/timescaledb-ha:pg16`)

| Table | Contenu | Clé primaire |
|-------|---------|-------------|
| `ml.piezo_station_embeddings` | 1 embedding/station piézo | `(code_bss, space)` |
| `ml.piezo_window_embeddings` | 1 embedding/fenêtre piézo | `(code_bss, window_start, space)` |
| `ml.hydro_station_embeddings` | 1 embedding/station hydro | `(code_station, space)` |
| `ml.hydro_window_embeddings` | 1 embedding/fenêtre hydro | `(code_station, window_start, space)` |
| `ml.clustering_runs` | Historique des exécutions clustering | `id SERIAL` |
| `ml.clustering_labels` | Labels + UMAP coords par exécution | `(run_id, station_id)` |

### 5.2 Colonnes station_embeddings

```
{id_col}, space, embedding vector(320), cluster_id, model_version, n_days, n_windows,
updated_at, umap_2d_x, umap_2d_y, umap_3d_x, umap_3d_y, umap_3d_z
```

### 5.3 Index

- **HNSW** sur `embedding` des station_embeddings (cosine, m=16, ef_construction=64) — similarité O(log n)
- **B-tree** sur `(id_col, window_start)` des window_embeddings — lookup par station

### 5.4 Volumétrie estimée

| | Piézo | Hydro | Total |
|--|-------|-------|-------|
| Stations (x2 spaces) | ~2 936 x 2 | ~2 535 x 2 | ~10 942 |
| Fenêtres (x2 spaces) | ~208K x 2 | ~161K x 2 | ~738K |
| Stockage estimé | ~510 MB | ~390 MB | ~900 MB |

---

## 6. Assets & Jobs Dagster

### 6.1 Assets (12)

**Code** : `src/hubeau_pipeline/assets/ml_assets.py`

| Asset | Groupe | Espace | Type | Trigger |
|-------|--------|--------|------|---------|
| `ml_piezo_multi_model_train` | ml_piezo | multi | Training | Manuel |
| `ml_piezo_uni_model_train` | ml_piezo | uni | Training | Manuel |
| `ml_hydro_multi_model_train` | ml_hydro | multi | Training | Manuel |
| `ml_hydro_uni_model_train` | ml_hydro | uni | Training | Manuel |
| `ml_piezo_multi_embeddings_update` | ml_piezo | multi | Encoding | Sensor |
| `ml_piezo_uni_embeddings_update` | ml_piezo | uni | Encoding | Sensor |
| `ml_hydro_multi_embeddings_update` | ml_hydro | multi | Encoding | Sensor |
| `ml_hydro_uni_embeddings_update` | ml_hydro | uni | Encoding | Sensor |
| `ml_piezo_multi_clusters` | ml_piezo | multi | Clustering | Après encode |
| `ml_piezo_uni_clusters` | ml_piezo | uni | Clustering | Après encode |
| `ml_hydro_multi_clusters` | ml_hydro | multi | Clustering | Après encode |
| `ml_hydro_uni_clusters` | ml_hydro | uni | Clustering | Après encode |

### 6.2 Jobs (8)

**Code** : `src/hubeau_pipeline/jobs/ml_jobs.py`

- 4 jobs de training (manuels) : `ml_{piezo|hydro}_{multi|uni}_train_job`
- 4 jobs nightly (encode + cluster) : `ml_{piezo|hydro}_{multi|uni}_embeddings_job`

### 6.3 Chaîne nightly (sensor-driven)

```
Bronze → shared_staging → piezo_daily ──→ dimensions
                                     ╰──→ ml_piezo_{multi,uni}_embeddings_job
                        → hydro_daily ──→ dimensions
                                     ╰──→ ml_hydro_{multi,uni}_embeddings_job
```

Les embeddings tournent en parallèle avec les dimensions.

---

## 7. Structure des fichiers

```
src/hubeau_pipeline/
├── ml/
│   ├── __init__.py
│   ├── ts2vec/                         # Vendorisé — TS2Vec core
│   │   ├── ts2vec.py                   # Classe principale (fit/encode/save/load)
│   │   ├── encoder.py                  # DilatedConvEncoder (CNN backbone)
│   │   ├── dilated_conv.py             # Blocs de convolution dilatée
│   │   ├── losses.py                   # Loss originale TS2Vec (remplacée par SoftCLT)
│   │   └── utils.py                    # Padding, split utilitaires
│   ├── softclt/                        # Vendorisé — SoftCLT loss
│   │   ├── losses.py                   # hierarchical_contrastive_loss (soft, drop-in)
│   │   ├── timelags.py                 # Matrices de timelag sigmoid/gaussian
│   │   └── hard_losses.py              # inst_CL_hard + temp_CL_hard (fallback)
│   └── latent_space/                   # Pipeline d'embedding
│       ├── encoder.py                  # SoftCLTEncoder (multi, GPU)
│       ├── rocket_encoder.py           # RocketEncoder (uni, CPU, MiniRocket+PCA)
│       ├── data.py                     # Loaders depuis Gold tables
│       ├── persistence.py              # pgvector CRUD, schema init, clustering storage
│       ├── clustering.py               # HDBSCAN + UMAP + métriques
│       └── tuning.py                   # Optuna hyperparameter optimization
├── assets/
│   └── ml_assets.py                    # 12 assets Dagster (train/encode/cluster x domain x space)
├── jobs/
│   └── ml_jobs.py                      # 8 jobs (4 training + 4 nightly)
```

---

## 8. Dépendances

| Package | Usage | GPU ? |
|---------|-------|-------|
| `torch` | SoftCLT training + encoding | Oui (CUDA) |
| `aeon` | MiniRocket encoder pour univarié | Non |
| `scikit-learn` | StandardScaler, PCA, métriques clustering | Non |
| `hdbscan` | Clustering density-based | Non |
| `umap-learn` | Pré-réduction dimensionnelle, visualisation | Non |
| `pgvector` | Extension PostgreSQL pour vecteurs | Non |
| `joblib` | Sérialisation modèles (scaler, rocket, pca) | Non |
| `optuna` | Tuning hyperparamètres (optionnel) | Non |

### Docker

- Worker : NVIDIA Container Toolkit pour GPU, volume externe `brgm_ml_models` monté sur `/var/ml/models`
- PostgreSQL : `CREATE EXTENSION vector` + `CREATE SCHEMA ml` dans `init.sql`

---

## 9. Commandes utiles

### Training (manuel via Dagster UI ou CLI)

```bash
# Training multivarié piézo (GPU, ~30min)
docker exec brgm-dlt-worker dagster job execute -j ml_piezo_multi_train_job

# Training univarié piézo (CPU, ~5min)
docker exec brgm-dlt-worker dagster job execute -j ml_piezo_uni_train_job
```

### Encoding + Clustering (normalement sensor-driven)

```bash
# Forcer un re-encodage piezo multi
docker exec brgm-dlt-worker dagster job execute -j ml_piezo_multi_embeddings_job
```

### Requêtes SQL

```sql
-- Stations les plus similaires (cosine, HNSW)
SELECT code_bss, embedding <=> (
    SELECT embedding FROM ml.piezo_station_embeddings
    WHERE code_bss = 'BSS001XXXX' AND space = 'multi'
) AS distance
FROM ml.piezo_station_embeddings
WHERE code_bss != 'BSS001XXXX' AND space = 'multi'
ORDER BY distance LIMIT 10;

-- Distribution des clusters
SELECT cluster_id, COUNT(*) as n_stations
FROM ml.piezo_station_embeddings
WHERE space = 'multi' AND cluster_id >= 0
GROUP BY cluster_id ORDER BY n_stations DESC;

-- Trajectoire temporelle d'une station
SELECT window_start, window_end, embedding
FROM ml.piezo_window_embeddings
WHERE code_bss = 'BSS001XXXX' AND space = 'multi'
ORDER BY window_start;

-- Dernière exécution de clustering
SELECT * FROM ml.clustering_runs
WHERE domain = 'piezo' AND is_default = TRUE
ORDER BY created_at DESC LIMIT 1;
```

---

## 10. Historique des choix techniques

| Date | Changement | Raison |
|------|-----------|--------|
| 2026-03-10 | Design initial avec TS2Vec | Apprentissage contrastif hiérarchique, self-supervised |
| 2026-03-11 | Benchmark 5 méthodes (TS2Vec, SoftCLT, tsfresh, Moment, Chronos) | SoftCLT meilleur silhouette (0.69) |
| 2026-03-12 | SoftCLT intégré en production | Monkey-patch loss, même architecture TS2Vec |
| 2026-03-12 | Dual-space (multi + uni) | Perspectives complémentaires hydro-climat vs signal pur |
| 2026-03-15 | MiniRocket+PCA remplace SoftCLT pour univarié | SoftCLT OOM en uni (matrice temporelle), MiniRocket plus rapide et sans GPU |
| 2026-03-15 | Batch MiniRocket transform (20K/batch) | Eviter OOM sur datasets > 100K fenêtres |
