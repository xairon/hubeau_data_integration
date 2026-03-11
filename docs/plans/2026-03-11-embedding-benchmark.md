# Benchmark Embeddings Séries Temporelles Hydrologiques

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Comparer 5 méthodes d'embedding sur un échantillon de stations piézo, avec un démonstrateur Streamlit interactif pour explorer les résultats. Projet Python **autonome** — zéro modification du code de l'entrepôt.

**Architecture:** Un projet Python autonome `benchmark/` à la racine du repo, avec son propre `pyproject.toml`, sa structure de package, et une app Streamlit. Se connecte en **lecture seule** à la base PostgreSQL de l'entrepôt via DSN. Les résultats sont sérialisés en `.parquet` pour alimenter l'UI sans recalcul.

**Tech Stack:** Python 3.11, uv (package manager), psycopg2-binary, pandas, numpy, torch, momentfm, chronos-forecasting, tsfresh, hdbscan, umap-learn, scikit-learn, matplotlib, seaborn, streamlit, plotly

**Spec de référence:** `docs/plans/2026-03-10-latent-space-piezometry.md` (plan TS2Vec original) + conversation veille du 2026-03-11

**Contraintes :**
- **NE PAS MODIFIER** : `src/`, `pyproject.toml` (racine), `docker-compose.yml`, `docker/`, `configs/`
- Connexion DB en **lecture seule** (SELECT uniquement sur `gold.*`)
- Projet autonome installable via `cd benchmark && pip install -e .`

---

## Structure du projet

```
benchmark/                              # PROJET AUTONOME
├── pyproject.toml                      # Dépendances propres au benchmark
├── README.md                           # Instructions d'exécution
├── .env.example                        # Template variables d'environnement
├── src/
│   └── embedding_benchmark/
│       ├── __init__.py
│       ├── config.py                   # Configuration centralisée (DSN, hyperparams)
│       ├── data_loader.py              # Chargement + échantillonnage depuis Gold
│       ├── evaluation.py               # 4 métriques non-supervisées
│       ├── methods/
│       │   ├── __init__.py             # Interface commune BaseMethod
│       │   ├── tsfresh_method.py       # Méthode 1 : features classiques
│       │   ├── moment_method.py        # Méthode 2 : MOMENT zero-shot
│       │   ├── chronos2_method.py      # Méthode 3 : Chronos-2 zero-shot
│       │   ├── ts2vec_method.py        # Méthode 4 : TS2Vec contrastif
│       │   └── softclt_method.py       # Méthode 5 : SoftCLT contrastif
│       └── vendors/
│           ├── __init__.py
│           ├── ts2vec/                 # Vendorisé depuis github
│           └── softclt/                # Vendorisé depuis github
│       └── ui/
│           ├── __init__.py
│           └── components.py               # Chargement résultats, UMAP cache
├── app/
│   ├── app.py                          # Point d'entrée Streamlit
│   └── pages/
│       ├── 1_comparison.py             # Tableau comparatif + radar
│       ├── 2_embeddings_explorer.py    # UMAP interactif par méthode
│       └── 3_station_similarity.py     # Recherche de stations similaires
├── notebooks/
│   └── run_benchmark.ipynb             # Notebook d'exécution du benchmark
├── results/                            # Résultats sérialisés (gitignored)
│   ├── .gitkeep
│   ├── embeddings/                     # .parquet par méthode
│   └── metrics/                        # .json par méthode
└── scripts/
    └── run_all.py                      # Script CLI pour tout lancer
```

---

## Chunk 1 : Setup projet autonome

### Task 1 : Créer la structure du projet

**Files:**
- Create: `benchmark/pyproject.toml`
- Create: `benchmark/.env.example`
- Create: `benchmark/src/embedding_benchmark/__init__.py`
- Create: `benchmark/results/.gitkeep`

- [ ] **Step 1: Créer l'arborescence**

```bash
mkdir -p benchmark/src/embedding_benchmark/methods
mkdir -p benchmark/src/embedding_benchmark/vendors/ts2vec
mkdir -p benchmark/src/embedding_benchmark/vendors/softclt
mkdir -p benchmark/src/embedding_benchmark/ui
mkdir -p benchmark/app/pages
mkdir -p benchmark/notebooks
mkdir -p benchmark/results/embeddings
mkdir -p benchmark/results/metrics
mkdir -p benchmark/scripts
touch benchmark/src/embedding_benchmark/__init__.py
touch benchmark/src/embedding_benchmark/methods/__init__.py
touch benchmark/src/embedding_benchmark/vendors/__init__.py
touch benchmark/src/embedding_benchmark/vendors/ts2vec/__init__.py
touch benchmark/src/embedding_benchmark/vendors/softclt/__init__.py
touch benchmark/src/embedding_benchmark/ui/__init__.py
touch benchmark/results/.gitkeep
touch benchmark/results/embeddings/.gitkeep
touch benchmark/results/metrics/.gitkeep
```

- [ ] **Step 2: Écrire pyproject.toml**

```toml
[project]
name = "embedding-benchmark"
version = "0.1.0"
description = "Benchmark de méthodes d'embedding pour séries temporelles hydrologiques"
requires-python = ">=3.11"
license = {text = "MIT"}

dependencies = [
    # DB
    "psycopg2-binary>=2.9.0",

    # Data
    "pandas>=2.2.0",
    "numpy>=1.26.0",
    "pyarrow>=15.0.0",

    # ML core
    "torch>=2.0.0",
    "scikit-learn>=1.3.0",
    "hdbscan>=0.8.33",
    "umap-learn>=0.5.0",
    "tqdm>=4.65.0",
    "joblib>=1.3.0",

    # Methods
    "momentfm>=0.1.4",
    "chronos-forecasting>=2.0.0",
    "tsfresh>=0.20.0",

    # Viz
    "matplotlib>=3.8.0",
    "seaborn>=0.13.0",
    "plotly>=5.18.0",

    # UI
    "streamlit>=1.30.0",

    # Notebook
    "jupyterlab>=4.0.0",

    # Config
    "python-dotenv>=1.0.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/embedding_benchmark"]

[tool.ruff]
line-length = 120
target-version = "py311"

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 3: Écrire .env.example**

```env
# Connexion PostgreSQL (entrepôt en lecture seule)
PG_HOST=localhost
PG_PORT=49502
PG_DB=postgres
PG_USER=postgres
PG_PASSWORD=changeme

# Benchmark
SAMPLE_SIZE=300
WINDOW_SIZE=365
STRIDE=90
EMBEDDING_DIM=320
SEED=42
```

- [ ] **Step 4: Écrire .gitignore pour results/**

Créer `benchmark/.gitignore` :

```gitignore
# Résultats (régénérables)
results/embeddings/*.parquet
results/metrics/*.json

# Modèles vendorisés (clonés à l'install)
# Note : on les commit car pas sur PyPI

# Python
__pycache__/
*.egg-info/
.venv/

# Notebook checkpoints
.ipynb_checkpoints/
```

- [ ] **Step 5: Commit structure**

```bash
git add benchmark/
git commit -m "chore(benchmark): scaffold autonomous benchmark project"
```

---

### Task 2 : Configuration centralisée

**Files:**
- Create: `benchmark/src/embedding_benchmark/config.py`

- [ ] **Step 1: Écrire config.py**

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
    pg_password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", "postgres"))

    # Benchmark params
    sample_size: int = field(default_factory=lambda: int(os.getenv("SAMPLE_SIZE", "300")))
    window_size: int = field(default_factory=lambda: int(os.getenv("WINDOW_SIZE", "365")))
    stride: int = field(default_factory=lambda: int(os.getenv("STRIDE", "90")))
    embedding_dim: int = field(default_factory=lambda: int(os.getenv("EMBEDDING_DIM", "320")))
    seed: int = field(default_factory=lambda: int(os.getenv("SEED", "42")))

    # Variables piézo
    piezo_cols: list[str] = field(default_factory=lambda: [
        "niveau_nappe_eau", "temperature_2m", "total_precipitation", "potential_evaporation"
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
    def metrics_dir(self):
        d = self.results_dir / "metrics"
        d.mkdir(parents=True, exist_ok=True)
        return d


# Singleton
cfg = BenchmarkConfig()
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/config.py
git commit -m "feat(benchmark): add centralized config from .env"
```

---

### Task 3 : Data loader

**Files:**
- Create: `benchmark/src/embedding_benchmark/data_loader.py`

- [ ] **Step 1: Écrire data_loader.py**

```python
"""
Chargement et échantillonnage des séries piézo depuis Gold.

Connexion en lecture seule via psycopg2 directement.
"""

import numpy as np
import pandas as pd
import psycopg2
from typing import Dict, Tuple

from .config import cfg


def get_eligible_stations(min_days: int = 730) -> pd.DataFrame:
    """
    Retourne les stations piézo éligibles avec métadonnées.

    Critères : ≥ min_days jours de données + dernière mesure ≥ 2024-01-01.

    Returns:
        DataFrame: code_bss, n_days, first_date, last_date, nature_eh, milieu_eh
    """
    query = """
        SELECT
            c.code_bss,
            COUNT(*) AS n_days,
            MIN(c.date) AS first_date,
            MAX(c.date) AS last_date,
            d.nature_eh,
            d.milieu_eh
        FROM gold.hubeau_daily_chroniques c
        LEFT JOIN gold.dim_piezo_stations d ON c.code_bss = d.code_bss
        GROUP BY c.code_bss, d.nature_eh, d.milieu_eh
        HAVING COUNT(*) >= %(min_days)s
           AND MAX(c.date) >= '2024-01-01'
        ORDER BY n_days DESC
    """
    with psycopg2.connect(cfg.dsn) as conn:
        return pd.read_sql(query, conn, params={"min_days": min_days})


def sample_stations(eligible: pd.DataFrame, n: int | None = None, seed: int | None = None) -> pd.DataFrame:
    """
    Échantillonnage stratifié par nature_eh (proportionnel).
    """
    n = n or cfg.sample_size
    seed = seed or cfg.seed
    rng = np.random.default_rng(seed)

    if len(eligible) <= n:
        return eligible.reset_index(drop=True)

    strata = eligible.groupby("nature_eh", observed=True)
    proportions = strata.size() / len(eligible)

    sampled = []
    for nature, group in strata:
        k = max(1, int(round(proportions[nature] * n)))
        k = min(k, len(group))
        idx = rng.choice(len(group), size=k, replace=False)
        sampled.append(group.iloc[idx])

    result = pd.concat(sampled)

    if len(result) > n:
        result = result.sample(n=n, random_state=seed)
    elif len(result) < n:
        remaining = eligible[~eligible.code_bss.isin(result.code_bss)]
        extra = remaining.sample(n=min(n - len(result), len(remaining)), random_state=seed)
        result = pd.concat([result, extra])

    return result.reset_index(drop=True)


def load_series(station_ids: list[str]) -> Tuple[Dict[str, np.ndarray], Dict[str, list]]:
    """
    Charge les séries multivariate pour les stations données.

    Returns:
        series: {code_bss: np.ndarray shape (T, 4) float32}
        dates:  {code_bss: [date, ...]}

    Les NaN sont interpolés linéairement puis remplis à 0.
    """
    placeholders = ",".join(["%s"] * len(station_ids))
    query = f"""
        SELECT code_bss, date,
               niveau_nappe_eau, temperature_2m,
               total_precipitation, potential_evaporation
        FROM gold.hubeau_daily_chroniques
        WHERE code_bss IN ({placeholders})
        ORDER BY code_bss, date
    """
    with psycopg2.connect(cfg.dsn) as conn:
        df = pd.read_sql(query, conn, params=tuple(station_ids))

    series = {}
    dates = {}
    for code_bss, group in df.groupby("code_bss"):
        arr = group[cfg.piezo_cols].interpolate().fillna(0).values.astype(np.float32)
        series[code_bss] = arr
        dates[code_bss] = group["date"].tolist()

    return series, dates


def make_windows(
    series: Dict[str, np.ndarray],
    dates: Dict[str, list],
    window_size: int | None = None,
    stride: int | None = None,
) -> Tuple[Dict[str, np.ndarray], Dict[str, list]]:
    """
    Découpe chaque série en fenêtres glissantes.

    Returns:
        windowed: {code_bss: np.ndarray shape (n_windows, window_size, n_vars)}
        win_dates: {code_bss: [(start_date, end_date), ...]}
    """
    window_size = window_size or cfg.window_size
    stride = stride or cfg.stride

    windowed = {}
    win_dates = {}
    for bss, arr in series.items():
        T = len(arr)
        if T < window_size:
            continue
        windows = []
        wdates = []
        for start in range(0, T - window_size + 1, stride):
            end = start + window_size
            windows.append(arr[start:end])
            d = dates.get(bss, [])
            if d:
                wdates.append((str(d[start]), str(d[end - 1])))
        windowed[bss] = np.stack(windows)
        win_dates[bss] = wdates
    return windowed, win_dates
```

- [ ] **Step 2: Vérifier la connexion DB**

```bash
cd benchmark
pip install -e .
python -c "
from embedding_benchmark.data_loader import get_eligible_stations
df = get_eligible_stations()
print(f'Stations éligibles: {len(df)}')
print(df['nature_eh'].value_counts())
"
```

Expected: ~2 935 stations éligibles.

- [ ] **Step 3: Commit**

```bash
git add benchmark/src/embedding_benchmark/data_loader.py
git commit -m "feat(benchmark): add data loader for piezo series from Gold tables"
```

---

### Task 4 : Module d'évaluation

**Files:**
- Create: `benchmark/src/embedding_benchmark/evaluation.py`

- [ ] **Step 1: Écrire evaluation.py**

```python
"""
Métriques d'évaluation non-supervisées pour comparer les embeddings.

4 métriques :
1. Silhouette score (HDBSCAN clusters)
2. ARI vs nature_eh (cohérence hydrogéologique)
3. Stabilité temporelle (corrélation cosinus fenêtres consécutives)
4. kNN cohérence (voisins proches = même nature_eh)
"""

import json
import numpy as np
import pandas as pd
import hdbscan
from pathlib import Path
from sklearn.metrics import silhouette_score, adjusted_rand_score
from sklearn.neighbors import NearestNeighbors
from typing import Dict

from .config import cfg


def cluster_hdbscan(embeddings: np.ndarray, min_cluster_size: int = 5) -> np.ndarray:
    """HDBSCAN clustering. Retourne labels (noise = -1)."""
    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=3, metric="euclidean")
    return clusterer.fit_predict(embeddings)


def eval_silhouette(embeddings: np.ndarray, labels: np.ndarray) -> float:
    """Silhouette score sur les points non-noise. [-1, 1], plus haut = mieux."""
    mask = labels >= 0
    if mask.sum() < 2 or len(set(labels[mask])) < 2:
        return -1.0
    return float(silhouette_score(embeddings[mask], labels[mask]))


def eval_ari_nature_eh(labels: np.ndarray, nature_eh: list[str]) -> float:
    """ARI entre clusters HDBSCAN et nature_eh. [-1, 1], 0 = random, 1 = parfait."""
    mask = np.array(labels) >= 0
    if mask.sum() < 2:
        return -1.0
    return float(adjusted_rand_score(np.array(nature_eh)[mask], labels[mask]))


def eval_temporal_stability(window_embeddings: Dict[str, np.ndarray]) -> float:
    """Corrélation cosinus moyenne entre fenêtres consécutives. [0, 1], haut = stable."""
    correlations = []
    for embs in window_embeddings.values():
        if len(embs) < 2:
            continue
        for i in range(len(embs) - 1):
            a, b = embs[i], embs[i + 1]
            norm_a, norm_b = np.linalg.norm(a), np.linalg.norm(b)
            if norm_a == 0 or norm_b == 0:
                continue
            correlations.append(np.dot(a, b) / (norm_a * norm_b))
    return float(np.mean(correlations)) if correlations else -1.0


def eval_knn_coherence(
    embeddings: np.ndarray,
    station_ids: list[str],
    station_meta: pd.DataFrame,
    k: int = 10,
    col: str = "nature_eh",
) -> float:
    """% de kNN qui partagent le même attribut. [0, 1], haut = cohérent."""
    nn = NearestNeighbors(n_neighbors=k + 1, metric="cosine")
    nn.fit(embeddings)
    _, indices = nn.kneighbors(embeddings)

    meta_map = dict(zip(station_meta["code_bss"], station_meta[col]))
    values = [meta_map.get(sid, "UNKNOWN") for sid in station_ids]

    coherences = []
    for i, neighbors in enumerate(indices):
        if values[i] == "UNKNOWN":
            continue
        neighbor_vals = [values[j] for j in neighbors[1:] if j < len(values)]
        if not neighbor_vals:
            continue
        coherences.append(sum(1 for v in neighbor_vals if v == values[i]) / len(neighbor_vals))

    return float(np.mean(coherences)) if coherences else -1.0


def run_full_evaluation(
    station_embeddings: np.ndarray,
    station_ids: list[str],
    station_meta: pd.DataFrame,
    window_embeddings: Dict[str, np.ndarray] | None = None,
    method_name: str = "unknown",
) -> Dict:
    """Lance les 4 métriques, retourne un dict, sauvegarde en JSON."""
    labels = cluster_hdbscan(station_embeddings)
    n_clusters = len(set(labels[labels >= 0]))
    n_noise = int((labels == -1).sum())

    nature_eh_list = []
    for sid in station_ids:
        match = station_meta.loc[station_meta["code_bss"] == sid, "nature_eh"]
        nature_eh_list.append(match.iloc[0] if len(match) > 0 else "UNKNOWN")

    result = {
        "method": method_name,
        "n_stations": len(station_ids),
        "n_clusters": n_clusters,
        "n_noise": n_noise,
        "noise_pct": round(n_noise / len(station_ids) * 100, 1),
        "silhouette": round(eval_silhouette(station_embeddings, labels), 4),
        "ari_nature_eh": round(eval_ari_nature_eh(labels, nature_eh_list), 4),
        "temporal_stability": round(
            eval_temporal_stability(window_embeddings) if window_embeddings else -1.0, 4
        ),
        "knn_coherence_nature_eh": round(
            eval_knn_coherence(station_embeddings, station_ids, station_meta), 4
        ),
    }

    # Sauvegarder les métriques en JSON
    out = cfg.metrics_dir / f"{method_name}.json"
    with open(out, "w") as f:
        json.dump(result, f, indent=2)

    return result


def save_embeddings(
    station_embeddings: np.ndarray,
    station_ids: list[str],
    labels: np.ndarray,
    method_name: str,
    station_meta: pd.DataFrame,
) -> Path:
    """Sauvegarde les embeddings station en Parquet (pour l'UI Streamlit)."""
    df = pd.DataFrame(station_embeddings, columns=[f"emb_{i}" for i in range(station_embeddings.shape[1])])
    df.insert(0, "code_bss", station_ids)
    df["cluster_id"] = labels

    # Joindre les métadonnées
    meta_cols = ["code_bss", "nature_eh", "milieu_eh", "n_days", "first_date", "last_date"]
    available = [c for c in meta_cols if c in station_meta.columns]
    df = df.merge(station_meta[available], on="code_bss", how="left")

    out = cfg.embeddings_dir / f"{method_name}.parquet"
    df.to_parquet(out, index=False)
    return out
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/evaluation.py
git commit -m "feat(benchmark): add evaluation module (silhouette, ARI, stability, kNN) + parquet export"
```

---

## Chunk 2 : Les 5 méthodes

### Task 5 : Interface commune des méthodes

**Files:**
- Modify: `benchmark/src/embedding_benchmark/methods/__init__.py`

- [ ] **Step 1: Écrire l'interface commune**

```python
"""Interface commune pour les méthodes d'embedding."""

from dataclasses import dataclass
import numpy as np
from typing import Dict, Tuple


@dataclass
class MethodResult:
    """Résultat standardisé d'une méthode d'embedding."""

    station_embeddings: np.ndarray      # (n_stations, dim)
    station_ids: list[str]              # [code_bss, ...]
    window_embeddings: Dict[str, np.ndarray]  # {code_bss: (n_windows, dim)}
    elapsed_seconds: float
    method_name: str
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/methods/__init__.py
git commit -m "feat(benchmark): add MethodResult dataclass"
```

---

### Task 6 : Méthode 1 — tsfresh

**Files:**
- Create: `benchmark/src/embedding_benchmark/methods/tsfresh_method.py`

- [ ] **Step 1: Écrire tsfresh_method.py**

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/methods/tsfresh_method.py
git commit -m "feat(benchmark): add tsfresh baseline method (EfficientFCParameters + PCA)"
```

---

### Task 7 : Méthode 2 — MOMENT

**Files:**
- Create: `benchmark/src/embedding_benchmark/methods/moment_method.py`

- [ ] **Step 1: Écrire moment_method.py**

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/methods/moment_method.py
git commit -m "feat(benchmark): add MOMENT method (zero-shot, univariate per channel + PCA)"
```

---

### Task 8 : Méthode 3 — Chronos-2

**Files:**
- Create: `benchmark/src/embedding_benchmark/methods/chronos2_method.py`

- [ ] **Step 1: Écrire chronos2_method.py**

```python
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
            # Chaque ligne = un canal de la série multivariate
            mv_tensor = torch.tensor(window.T, dtype=torch.float32)  # (4, 365)
            with torch.no_grad():
                emb, _ = pipeline.embed([mv_tensor])
            # emb: (1, n_patches, d_model) → mean pool sur patches
            # Note : vérifier la shape à l'exécution, adapter si nécessaire
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
```

> **Note API Chronos-2** : L'API `pipeline.embed()` est documentée depuis la v2.0. Si la signature change, consulter la [discussion GitHub #354](https://github.com/amazon-science/chronos-forecasting/discussions/354) et adapter le code. Le model ID `"amazon/chronos-2"` est le repo HuggingFace officiel.

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/methods/chronos2_method.py
git commit -m "feat(benchmark): add Chronos-2 method (zero-shot, multivariate native)"
```

---

### Task 9 : Vendoriser TS2Vec et SoftCLT

**Files:**
- Create: `benchmark/src/embedding_benchmark/vendors/ts2vec/*.py`
- Create: `benchmark/src/embedding_benchmark/vendors/softclt/losses.py`

- [ ] **Step 1: Cloner et copier TS2Vec**

```bash
VENDORS_DIR="$(pwd)/benchmark/src/embedding_benchmark/vendors"
cd /tmp
git clone --depth 1 https://github.com/zhihanyue/ts2vec.git ts2vec_repo
cp ts2vec_repo/ts2vec.py "$VENDORS_DIR/ts2vec/ts2vec.py"
cp ts2vec_repo/encoder.py "$VENDORS_DIR/ts2vec/encoder.py"
cp ts2vec_repo/losses.py "$VENDORS_DIR/ts2vec/losses.py"
cp ts2vec_repo/utils.py "$VENDORS_DIR/ts2vec/utils.py"
rm -rf /tmp/ts2vec_repo
```

- [ ] **Step 2: Fixer les imports dans le code vendorisé**

Les fichiers TS2Vec utilisent des imports relatifs entre eux. Vérifier et ajuster :
- `ts2vec.py` : `from .encoder import ...` et `from .losses import ...`
- `encoder.py` : `from .utils import ...` (si nécessaire)

```bash
python -c "
import sys; sys.path.insert(0, 'benchmark/src')
from embedding_benchmark.vendors.ts2vec.ts2vec import TS2Vec
print('TS2Vec import OK')
"
```

Expected: OK. Si erreur d'import, ajuster les imports relatifs.

- [ ] **Step 3: Cloner et copier SoftCLT**

```bash
cd /tmp
git clone --depth 1 https://github.com/seunghan96/softclt.git softclt_repo
cp softclt_repo/softclt/losses.py "$VENDORS_DIR/softclt/losses.py"
rm -rf /tmp/softclt_repo
```

- [ ] **Step 4: Inspecter et adapter les imports pour le monkey-patch SoftCLT**

SoftCLT remplace `hierarchical_contrastive_loss` de TS2Vec. Pour que le monkey-patch fonctionne, TS2Vec doit importer la loss **via le module** (pas comme binding direct).

```bash
# Voir quelle fonction la loss SoftCLT exporte
grep "^def " "$VENDORS_DIR/softclt/losses.py"

# Voir comment TS2Vec importe sa loss (CRITIQUE pour le monkey-patch)
grep "contrastive_loss\|from.*losses" "$VENDORS_DIR/ts2vec/ts2vec.py"
```

**Si TS2Vec utilise `from .losses import hierarchical_contrastive_loss`** (binding direct) :
→ Le monkey-patch module ne fonctionnera pas. Modifier `ts2vec.py` pour utiliser `from . import losses` puis appeler `losses.hierarchical_contrastive_loss(...)` dans le code. C'est une modification d'une seule ligne dans le code vendorisé.

**Si TS2Vec utilise `from . import losses`** (import module) :
→ Le monkey-patch fonctionne directement, rien à changer.

- [ ] **Step 5: Commit**

```bash
git add benchmark/src/embedding_benchmark/vendors/
git commit -m "chore(benchmark): vendorize TS2Vec and SoftCLT source code"
```

---

### Task 10 : Méthode 4 — TS2Vec

**Files:**
- Create: `benchmark/src/embedding_benchmark/methods/ts2vec_method.py`

- [ ] **Step 1: Écrire ts2vec_method.py**

```python
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
    train_data = [scaled[bss] for bss in station_ids]

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
        arr = scaled[bss]
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
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/methods/ts2vec_method.py
git commit -m "feat(benchmark): add TS2Vec contrastive method (trained on our data)"
```

---

### Task 11 : Méthode 5 — SoftCLT

**Files:**
- Create: `benchmark/src/embedding_benchmark/methods/softclt_method.py`

- [ ] **Step 1: Écrire softclt_method.py**

```python
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


def _patch_softclt_loss(model: TS2Vec) -> bool:
    """
    Remplace la loss de TS2Vec par celle de SoftCLT.

    SoftCLT modifie `hierarchical_contrastive_loss` dans losses.py.
    On patche le module importé par ts2vec.py.

    Returns True si le patch a réussi, False sinon.
    """
    try:
        from ..vendors.softclt import losses as softclt_losses

        # Identifier la fonction de loss SoftCLT
        # SoftCLT exporte typiquement `soft_contrastive_loss` ou patche `hierarchical_contrastive_loss`
        if hasattr(softclt_losses, "hierarchical_contrastive_loss"):
            # SoftCLT fournit un remplacement direct
            import embedding_benchmark.vendors.ts2vec.losses as ts2vec_losses
            ts2vec_losses.hierarchical_contrastive_loss = softclt_losses.hierarchical_contrastive_loss
            return True
        elif hasattr(softclt_losses, "soft_contrastive_loss"):
            import embedding_benchmark.vendors.ts2vec.losses as ts2vec_losses
            ts2vec_losses.hierarchical_contrastive_loss = softclt_losses.soft_contrastive_loss
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
    train_data = [scaled[bss] for bss in station_ids]

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = TS2Vec(
        input_dims=n_vars,
        output_dims=cfg.embedding_dim,
        hidden_dims=cfg.embedding_dim,
        depth=depth,
        device=device,
    )

    # Patch la loss
    patched = _patch_softclt_loss(model)
    method_name = "SoftCLT" if patched else "SoftCLT (fallback TS2Vec)"

    model.fit(train_data, n_epochs=n_epochs, lr=lr, batch_size=batch_size, verbose=True)

    # Encoding identique à TS2Vec
    window_embeddings: Dict[str, np.ndarray] = {}
    station_emb_list = []
    valid_ids = []

    for bss in station_ids:
        arr = scaled[bss]
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
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/src/embedding_benchmark/methods/softclt_method.py
git commit -m "feat(benchmark): add SoftCLT method (soft loss patch on TS2Vec)"
```

---

## Chunk 3 : Script d'exécution + Notebook

### Task 12 : Script CLI run_all.py

**Files:**
- Create: `benchmark/scripts/run_all.py`

- [ ] **Step 1: Écrire run_all.py**

```python
"""
Script CLI pour exécuter le benchmark complet.

Usage:
    cd benchmark
    python scripts/run_all.py                     # Toutes les méthodes
    python scripts/run_all.py --methods tsfresh MOMENT  # Méthodes spécifiques
    python scripts/run_all.py --sample-size 50    # Petit échantillon (debug)
"""

import argparse
import json
import sys
import time
from pathlib import Path

# Ajouter src/ au path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from embedding_benchmark.config import cfg
from embedding_benchmark.data_loader import get_eligible_stations, sample_stations, load_series
from embedding_benchmark.evaluation import run_full_evaluation, save_embeddings, cluster_hdbscan


METHODS = {
    "tsfresh": "embedding_benchmark.methods.tsfresh_method",
    "MOMENT": "embedding_benchmark.methods.moment_method",
    "Chronos-2": "embedding_benchmark.methods.chronos2_method",
    "TS2Vec": "embedding_benchmark.methods.ts2vec_method",
    "SoftCLT": "embedding_benchmark.methods.softclt_method",
}


def run_method(name: str, series, dates):
    """Importe et exécute une méthode par son nom."""
    import importlib
    module = importlib.import_module(METHODS[name])
    return module.run(series, dates)


def main():
    parser = argparse.ArgumentParser(description="Benchmark embeddings séries temporelles")
    parser.add_argument("--methods", nargs="+", default=list(METHODS.keys()),
                        choices=list(METHODS.keys()), help="Méthodes à exécuter")
    parser.add_argument("--sample-size", type=int, default=None,
                        help=f"Taille échantillon (défaut: {cfg.sample_size})")
    args = parser.parse_args()

    if args.sample_size:
        cfg.sample_size = args.sample_size

    print(f"{'='*60}")
    print(f"BENCHMARK EMBEDDINGS — {len(args.methods)} méthodes, {cfg.sample_size} stations")
    print(f"{'='*60}\n")

    # 1. Charger les données
    print("Chargement des stations éligibles...")
    eligible = get_eligible_stations()
    print(f"  {len(eligible)} stations éligibles")

    sample = sample_stations(eligible)
    print(f"  {len(sample)} stations échantillonnées")

    print("Chargement des séries temporelles...")
    series, dates = load_series(sample["code_bss"].tolist())
    print(f"  {len(series)} séries chargées\n")

    # 2. Exécuter chaque méthode
    all_results = []
    for method_name in args.methods:
        print(f"\n{'='*60}")
        print(f"MÉTHODE : {method_name}")
        print(f"{'='*60}")

        try:
            result = run_method(method_name, series, dates)

            # Évaluation
            labels = cluster_hdbscan(result.station_embeddings)
            metrics = run_full_evaluation(
                result.station_embeddings,
                result.station_ids,
                sample,
                window_embeddings=result.window_embeddings,
                method_name=result.method_name,
            )
            metrics["time_seconds"] = round(result.elapsed_seconds, 1)
            all_results.append(metrics)

            # Sauvegarder embeddings en parquet
            save_embeddings(
                result.station_embeddings,
                result.station_ids,
                labels,
                result.method_name,
                sample,
            )

            print(f"\n  Résultats {result.method_name}:")
            for k, v in metrics.items():
                if k != "method":
                    print(f"    {k}: {v}")

        except Exception as e:
            print(f"\n  ERREUR {method_name}: {e}")
            import traceback
            traceback.print_exc()

    # 3. Résumé
    if all_results:
        print(f"\n\n{'='*60}")
        print("RÉSUMÉ COMPARATIF")
        print(f"{'='*60}\n")

        import pandas as pd
        df = pd.DataFrame(all_results)
        print(df.to_string(index=False))

        # Sauvegarder résumé
        summary_path = cfg.metrics_dir / "summary.json"
        with open(summary_path, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\nRésultats sauvegardés dans {summary_path}")
        print(f"Embeddings sauvegardés dans {cfg.embeddings_dir}/")
        print(f"\nPour lancer l'UI : cd benchmark && streamlit run app/app.py")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Tester avec un petit échantillon**

```bash
cd benchmark
python scripts/run_all.py --methods tsfresh --sample-size 10
```

Expected: exécution en ~1 min, fichiers dans `results/`.

- [ ] **Step 3: Commit**

```bash
git add benchmark/scripts/run_all.py
git commit -m "feat(benchmark): add CLI script to run all methods and export results"
```

---

### Task 13 : Notebook d'exécution

**Files:**
- Create: `benchmark/notebooks/run_benchmark.ipynb`

- [ ] **Step 1: Créer le notebook**

Le notebook utilise les mêmes modules que `run_all.py`, mais avec une exécution cellule par cellule et des visualisations inline. Structure :

| Cellule | Contenu |
|---------|---------|
| 1 | Imports + config |
| 2 | Chargement données + stats |
| 3-7 | Une cellule par méthode (`%%time` + résultats) |
| 8 | Tableau comparatif (pandas DataFrame) |
| 9 | Radar chart |
| 10 | UMAP 2×5 grid (clusters + nature_eh) |
| 11 | Score composite + recommandation |

Le code de chaque cellule suit exactement le pattern :

```python
%%time
from embedding_benchmark.methods import tsfresh_method
result = tsfresh_method.run(series, dates)
metrics = run_full_evaluation(result.station_embeddings, result.station_ids, sample,
                               window_embeddings=result.window_embeddings, method_name=result.method_name)
metrics["time_seconds"] = round(result.elapsed_seconds, 1)
labels = cluster_hdbscan(result.station_embeddings)
save_embeddings(result.station_embeddings, result.station_ids, labels, result.method_name, sample)
all_results.append(metrics)
print(metrics)
```

> Le notebook est un complément interactif au script CLI. Les deux produisent les mêmes fichiers `results/`.

- [ ] **Step 2: Commit**

```bash
git add benchmark/notebooks/
git commit -m "feat(benchmark): add Jupyter notebook for interactive benchmark execution"
```

---

## Chunk 4 : Démonstrateur Streamlit

### Task 14 : Composants partagés + page d'accueil

**Files:**
- Create: `benchmark/src/embedding_benchmark/ui/__init__.py`
- Create: `benchmark/src/embedding_benchmark/ui/components.py`
- Create: `benchmark/app/app.py`

- [ ] **Step 1: Écrire app.py (point d'entrée)**

```python
"""
Démonstrateur Streamlit — Benchmark Embeddings Hydrologiques.

Lancement : cd benchmark && streamlit run app/app.py
Prérequis : avoir exécuté run_all.py ou le notebook pour générer results/
"""

import streamlit as st
from pathlib import Path
import sys

# Ajouter src/ au path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

st.set_page_config(
    page_title="Benchmark Embeddings Hydro",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("💧 Benchmark Embeddings — Séries Temporelles Hydrologiques")

st.markdown("""
### Objectif

Comparer **5 méthodes d'embedding** sur ~300 stations piézométriques :

| # | Méthode | Type |
|---|---------|------|
| 1 | **tsfresh** | Features classiques (baseline) |
| 2 | **MOMENT** | Foundation model zero-shot (univariate) |
| 3 | **Chronos-2** | Foundation model zero-shot (multivariate) |
| 4 | **TS2Vec** | Contrastif hiérarchique (entraîné) |
| 5 | **SoftCLT** | Contrastif soft assignments (entraîné) |

### Navigation

- **Comparaison** : tableau récapitulatif + radar chart
- **Exploration embeddings** : UMAP interactif coloré par cluster/nature_eh
- **Similarité stations** : recherche des k plus proches voisins
""")

# Vérifier que les résultats existent
results_dir = Path(__file__).parent.parent / "results"
metrics_dir = results_dir / "metrics"
embeddings_dir = results_dir / "embeddings"

if not metrics_dir.exists() or not list(metrics_dir.glob("*.json")):
    st.warning("⚠️ Aucun résultat trouvé. Exécuter d'abord le benchmark :")
    st.code("cd benchmark && python scripts/run_all.py", language="bash")
else:
    n_methods = len(list(metrics_dir.glob("*.json"))) - (1 if (metrics_dir / "summary.json").exists() else 0)
    n_embeddings = len(list(embeddings_dir.glob("*.parquet")))
    st.success(f"✅ {n_methods} méthodes évaluées, {n_embeddings} fichiers embeddings disponibles.")
```

- [ ] **Step 2: Écrire ui/components.py**

> Ce module vit dans le package `embedding_benchmark` pour que les pages Streamlit puissent l'importer via `from embedding_benchmark.ui.components import ...` (le path `benchmark/src/` est ajouté au sys.path).

```python
"""Composants partagés pour le chargement des résultats et la projection UMAP."""

import json
import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path
from umap import UMAP

from ..config import cfg


METRICS_DIR = cfg.metrics_dir
EMBEDDINGS_DIR = cfg.embeddings_dir


def load_all_metrics() -> pd.DataFrame:
    """Charge tous les JSON de métriques en un DataFrame."""
    records = []
    for f in sorted(METRICS_DIR.glob("*.json")):
        if f.name == "summary.json":
            continue
        with open(f) as fp:
            records.append(json.load(fp))
    return pd.DataFrame(records)


def load_embeddings(method_name: str) -> pd.DataFrame:
    """Charge le parquet d'embeddings d'une méthode."""
    path = EMBEDDINGS_DIR / f"{method_name}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def get_embedding_columns(df: pd.DataFrame) -> list[str]:
    """Retourne la liste des colonnes embedding (emb_0, emb_1, ...)."""
    return [c for c in df.columns if c.startswith("emb_")]


@st.cache_data(ttl=3600)
def compute_umap(method_name: str, n_components: int = 2) -> np.ndarray:
    """Calcule et cache la projection UMAP pour une méthode (cache Streamlit, TTL 1h)."""
    df = load_embeddings(method_name)
    if df.empty:
        return np.array([])
    emb_cols = get_embedding_columns(df)
    embeddings = df[emb_cols].values
    reducer = UMAP(n_components=n_components, random_state=42, metric="cosine")
    return reducer.fit_transform(embeddings)


def available_methods() -> list[str]:
    """Liste les méthodes pour lesquelles on a des embeddings."""
    return sorted([f.stem for f in EMBEDDINGS_DIR.glob("*.parquet")])
```

- [ ] **Step 3: Commit**

```bash
git add benchmark/src/embedding_benchmark/ui/ benchmark/app/app.py
git commit -m "feat(benchmark): add Streamlit app entry point and shared components"
```

---

### Task 15 : Page 1 — Comparaison des méthodes

**Files:**
- Create: `benchmark/app/pages/1_comparison.py`

- [ ] **Step 1: Écrire 1_comparison.py**

```python
"""Page 1 : Tableau comparatif + radar chart."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from embedding_benchmark.ui.components import load_all_metrics

st.header("📊 Comparaison des méthodes")

df = load_all_metrics()
if df.empty:
    st.warning("Aucun résultat. Exécuter le benchmark d'abord.")
    st.stop()

# --- Tableau ---
st.subheader("Métriques")

metric_cols = ["method", "n_clusters", "noise_pct", "silhouette",
               "ari_nature_eh", "temporal_stability", "knn_coherence_nature_eh"]
if "time_seconds" in df.columns:
    metric_cols.append("time_seconds")
available_cols = [c for c in metric_cols if c in df.columns]

st.dataframe(
    df[available_cols].sort_values("silhouette", ascending=False),
    use_container_width=True,
    hide_index=True,
)

# --- Score composite ---
st.subheader("Score composite")

weights = {
    "silhouette": st.sidebar.slider("Poids Silhouette", 0.0, 1.0, 0.3, 0.05),
    "ari_nature_eh": st.sidebar.slider("Poids ARI nature_eh", 0.0, 1.0, 0.3, 0.05),
    "temporal_stability": st.sidebar.slider("Poids Stabilité temp.", 0.0, 1.0, 0.2, 0.05),
    "knn_coherence_nature_eh": st.sidebar.slider("Poids kNN cohérence", 0.0, 1.0, 0.2, 0.05),
}

df_score = df.copy()
for col in weights:
    if col in df_score.columns:
        mi, ma = df_score[col].min(), df_score[col].max()
        df_score[col + "_norm"] = (df_score[col] - mi) / (ma - mi) if ma > mi else 0.5

total_weight = sum(weights.values())
df_score["score_composite"] = sum(
    df_score.get(col + "_norm", 0.5) * (w / total_weight)
    for col, w in weights.items()
)
df_score = df_score.sort_values("score_composite", ascending=False)

st.dataframe(
    df_score[["method", "score_composite"]].reset_index(drop=True),
    use_container_width=True,
    hide_index=True,
)

st.success(f"🏆 Méthode recommandée : **{df_score.iloc[0]['method']}** (score = {df_score.iloc[0]['score_composite']:.4f})")

# --- Radar chart ---
st.subheader("Radar chart")

metrics = list(weights.keys())
labels = ["Silhouette", "ARI nature_eh", "Stabilité temp.", "kNN cohérence"]

fig = go.Figure()
for _, row in df_score.iterrows():
    values = [row.get(m + "_norm", 0.5) for m in metrics]
    values.append(values[0])  # fermer le polygone
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=labels + [labels[0]],
        fill="toself",
        name=row["method"],
        opacity=0.6,
    ))

fig.update_layout(
    polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
    showlegend=True,
    height=500,
)
st.plotly_chart(fig, use_container_width=True)
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/app/pages/1_comparison.py
git commit -m "feat(benchmark): add comparison page (table, score composite, radar chart)"
```

---

### Task 16 : Page 2 — Exploration des embeddings

**Files:**
- Create: `benchmark/app/pages/2_embeddings_explorer.py`

- [ ] **Step 1: Écrire 2_embeddings_explorer.py**

```python
"""Page 2 : Exploration interactive UMAP des embeddings."""

import streamlit as st
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from embedding_benchmark.ui.components import available_methods, load_embeddings, compute_umap

st.header("🗺️ Exploration des embeddings")

methods = available_methods()
if not methods:
    st.warning("Aucun embedding disponible.")
    st.stop()

# Sélection
col1, col2 = st.columns(2)
with col1:
    method = st.selectbox("Méthode", methods)
with col2:
    color_by = st.selectbox("Colorer par", ["cluster_id", "nature_eh", "milieu_eh", "n_days"])

# Charger
df = load_embeddings(method)
if df.empty:
    st.warning(f"Pas d'embeddings pour {method}")
    st.stop()

# UMAP
coords = compute_umap(method)
if len(coords) == 0:
    st.error("UMAP échoué")
    st.stop()

df["umap_1"] = coords[:, 0]
df["umap_2"] = coords[:, 1]

# Préparer la colonne couleur
if color_by not in df.columns:
    st.warning(f"Colonne '{color_by}' absente")
    color_by = "cluster_id"

if color_by in ["cluster_id"]:
    df[color_by] = df[color_by].astype(str)

# Plot
fig = px.scatter(
    df, x="umap_1", y="umap_2",
    color=color_by,
    hover_data=["code_bss", "nature_eh", "milieu_eh", "n_days"],
    title=f"{method} — UMAP (coloré par {color_by})",
    width=900, height=700,
    opacity=0.7,
)
fig.update_traces(marker=dict(size=5))
fig.update_layout(legend=dict(itemsizing="constant"))

st.plotly_chart(fig, use_container_width=True)

# Stats
st.subheader("Statistiques")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Stations", len(df))
with col2:
    n_clusters = df["cluster_id"].nunique() if "cluster_id" in df.columns else 0
    st.metric("Clusters", n_clusters)
with col3:
    noise = (df.get("cluster_id", "").astype(str) == "-1").sum()
    st.metric("Noise", f"{noise} ({noise/len(df)*100:.1f}%)")

# Distribution clusters
if "cluster_id" in df.columns:
    st.subheader("Distribution des clusters")
    cluster_counts = df["cluster_id"].value_counts().reset_index()
    cluster_counts.columns = ["cluster_id", "count"]
    fig_bar = px.bar(cluster_counts, x="cluster_id", y="count", title="Stations par cluster")
    st.plotly_chart(fig_bar, use_container_width=True)
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/app/pages/2_embeddings_explorer.py
git commit -m "feat(benchmark): add UMAP embedding explorer page (interactive Plotly)"
```

---

### Task 17 : Page 3 — Similarité entre stations

**Files:**
- Create: `benchmark/app/pages/3_station_similarity.py`

- [ ] **Step 1: Écrire 3_station_similarity.py**

```python
"""Page 3 : Recherche de stations similaires (kNN dans l'espace embedding)."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from sklearn.neighbors import NearestNeighbors
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from embedding_benchmark.ui.components import available_methods, load_embeddings, get_embedding_columns, compute_umap

st.header("🔍 Recherche de stations similaires")

methods = available_methods()
if not methods:
    st.warning("Aucun embedding disponible.")
    st.stop()

method = st.selectbox("Méthode", methods, key="sim_method")
df = load_embeddings(method)
if df.empty:
    st.stop()

# Sélection station
station = st.selectbox("Station de référence", sorted(df["code_bss"].tolist()), key="sim_station")
k = st.slider("Nombre de voisins", 3, 30, 10)

# kNN
emb_cols = get_embedding_columns(df)
embeddings = df[emb_cols].values
nn = NearestNeighbors(n_neighbors=k + 1, metric="cosine")
nn.fit(embeddings)

idx = df[df["code_bss"] == station].index[0]
distances, indices = nn.kneighbors(embeddings[idx:idx+1])

# Résultats
neighbors = df.iloc[indices[0][1:]].copy()
neighbors["distance"] = distances[0][1:]
neighbors["rank"] = range(1, k + 1)

st.subheader(f"Top {k} stations similaires à {station}")

display_cols = ["rank", "code_bss", "distance"]
for c in ["nature_eh", "milieu_eh", "cluster_id", "n_days"]:
    if c in neighbors.columns:
        display_cols.append(c)

st.dataframe(neighbors[display_cols], use_container_width=True, hide_index=True)

# Métadonnées de la station de référence
ref = df[df["code_bss"] == station].iloc[0]
st.subheader("Station de référence")
meta_cols = ["code_bss", "nature_eh", "milieu_eh", "cluster_id", "n_days"]
ref_meta = {c: ref.get(c, "N/A") for c in meta_cols if c in ref.index}
st.json(ref_meta)

# Cohérence
if "nature_eh" in neighbors.columns and "nature_eh" in ref.index:
    same_nature = (neighbors["nature_eh"] == ref["nature_eh"]).sum()
    st.metric(
        "Cohérence nature_eh",
        f"{same_nature}/{k} ({same_nature/k*100:.0f}%)",
    )

# UMAP avec highlight
coords = compute_umap(method)
if len(coords) > 0:
    df_plot = df.copy()
    df_plot["umap_1"] = coords[:, 0]
    df_plot["umap_2"] = coords[:, 1]
    df_plot["role"] = "autre"
    df_plot.loc[df_plot["code_bss"] == station, "role"] = "référence"
    df_plot.loc[df_plot["code_bss"].isin(neighbors["code_bss"]), "role"] = "voisin"

    fig = px.scatter(
        df_plot, x="umap_1", y="umap_2",
        color="role",
        color_discrete_map={"autre": "lightgray", "référence": "red", "voisin": "blue"},
        hover_data=["code_bss", "nature_eh"],
        title=f"UMAP — {station} et ses {k} voisins ({method})",
        opacity=0.7,
        height=600,
    )
    fig.update_traces(marker=dict(size=5), selector=dict(name="autre"))
    fig.update_traces(marker=dict(size=12), selector=dict(name="référence"))
    fig.update_traces(marker=dict(size=8), selector=dict(name="voisin"))
    st.plotly_chart(fig, use_container_width=True)
```

- [ ] **Step 2: Vérifier que l'app Streamlit se lance**

```bash
cd benchmark
streamlit run app/app.py --server.headless true
```

Expected: app sur `http://localhost:8501`, page d'accueil affichée. Les pages 1-3 sont dans la sidebar.

- [ ] **Step 3: Commit**

```bash
git add benchmark/app/pages/
git commit -m "feat(benchmark): add station similarity page (kNN search + UMAP highlight)"
```

---

### Task 18 : README du projet benchmark

**Files:**
- Create: `benchmark/README.md`

- [ ] **Step 1: Écrire README.md**

```markdown
# Benchmark Embeddings — Séries Temporelles Hydrologiques

Comparaison de 5 méthodes d'embedding pour les chroniques piézométriques.

## Méthodes

| # | Méthode | Type | GPU | Training |
|---|---------|------|-----|----------|
| 1 | tsfresh | Features classiques | Non | Non |
| 2 | MOMENT | Foundation model (zero-shot) | Optionnel | Non |
| 3 | Chronos-2 | Foundation model (zero-shot) | Optionnel | Non |
| 4 | TS2Vec | Contrastif hiérarchique | Optionnel | Oui |
| 5 | SoftCLT | Contrastif soft (ICLR 2024) | Optionnel | Oui |

## Installation

```bash
cd benchmark
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env
# Éditer .env avec les credentials PostgreSQL
```

## Exécution

```bash
# Toutes les méthodes
python scripts/run_all.py

# Méthodes spécifiques
python scripts/run_all.py --methods tsfresh MOMENT

# Petit échantillon (debug)
python scripts/run_all.py --methods tsfresh --sample-size 10

# Notebook interactif
jupyter lab notebooks/run_benchmark.ipynb
```

## UI Streamlit

```bash
streamlit run app/app.py
```

3 pages :
- **Comparaison** : tableau, score composite (pondération interactive), radar chart
- **Exploration** : UMAP interactif coloré par cluster/nature_eh/milieu_eh
- **Similarité** : recherche kNN, cohérence, UMAP avec highlight

## Prérequis

- Python 3.11+
- Accès PostgreSQL à l'entrepôt Hub'Eau (lecture seule sur `gold.*`)
- ~4 GB RAM (300 stations, CPU)
- GPU optionnel (accélère MOMENT, Chronos-2, TS2Vec, SoftCLT)
```

- [ ] **Step 2: Commit**

```bash
git add benchmark/README.md
git commit -m "docs(benchmark): add README with install, usage, and UI instructions"
```

---

## Résumé des fichiers

```
benchmark/                                  # PROJET AUTONOME
├── pyproject.toml                          # Dépendances propres
├── README.md                              # Instructions
├── .env.example                           # Template credentials
├── .gitignore                             # Ignore results, venv, checkpoints
├── src/embedding_benchmark/
│   ├── __init__.py
│   ├── config.py                          # BenchmarkConfig (DSN, hyperparams)
│   ├── data_loader.py                     # Chargement Gold + échantillonnage
│   ├── evaluation.py                      # 4 métriques + export parquet/json
│   ├── methods/
│   │   ├── __init__.py                    # MethodResult dataclass
│   │   ├── tsfresh_method.py              # Baseline features
│   │   ├── moment_method.py               # Foundation model univariate
│   │   ├── chronos2_method.py             # Foundation model multivariate
│   │   ├── ts2vec_method.py               # Contrastif hiérarchique
│   │   └── softclt_method.py              # Contrastif soft
│   ├── vendors/
│   │   ├── ts2vec/                         # Vendorisé (5 fichiers)
│   │   └── softclt/                        # Vendorisé (1 fichier loss)
│   └── ui/
│       └── components.py                   # Chargement résultats + UMAP cache
├── app/
│   ├── app.py                             # Streamlit entry point
│   └── pages/
│       ├── 1_comparison.py                # Tableau + radar + score composite
│       ├── 2_embeddings_explorer.py       # UMAP interactif
│       └── 3_station_similarity.py        # kNN + highlight UMAP
├── notebooks/
│   └── run_benchmark.ipynb                # Exécution interactive
├── results/                               # Sortie (gitignored sauf .gitkeep)
│   ├── embeddings/*.parquet
│   └── metrics/*.json
└── scripts/
    └── run_all.py                         # CLI complet
```

## Graphe de dépendances

```
T1 (scaffold) → T2 (config) → T3 (data_loader) ──┐
                                T4 (evaluation) ───┤
                                T5 (interface) ────┤
                                T6 (tsfresh) ──────┤
                                T7 (moment) ───────┤
                                T8 (chronos2) ─────┼→ T12 (run_all.py) → T13 (notebook)
                T9 (vendorize) → T10 (ts2vec) ─────┤
                               → T11 (softclt) ────┘
                                T14 (app.py) ──────┐
                                T15 (comparison) ──┼→ T18 (README)
                                T16 (explorer) ────┤
                                T17 (similarity) ──┘
```

T3-T11 et T14-T17 sont parallélisables. Le chemin critique est T1 → T2 → T9 → T10 → T12 → T13.
