# Plan : Embeddings Stations Piézo + Hydro (TS2Vec)

> **Date** : 2026-03-10
> **Statut** : Validé
> **Méthode** : TS2Vec (apprentissage contrastif hiérarchique)
> **Sortie** : 4 tables `ml.*` (station + fenêtres × 2 domaines)
> **Mise à jour** : Nightly sensor-driven (encode dernière fenêtre)
> **GPU** : NVIDIA Container Toolkit sur le worker Docker

---

## 1. Chiffres réels de l'entrepôt

### Sources

| | `gold.hubeau_daily_chroniques` | `gold.hydro_daily_chroniques` |
|--|-------------------------------|-------------------------------|
| Lignes totales | 23.4M | 22.2M |
| Stations totales | 18 745 | 4 672 |
| Stations ≥ 2 ans | 3 978 | 4 084 |
| **Éligibles (≥ 2 ans + active 2024+)** | **2 935** | **2 535** |
| Durée médiane (éligibles) | 18.0 ans (6 583j) | 15.7 ans (5 728j) |
| Durée min / max | 2.0 / 55.2 ans | 2.0 / 57.7 ans |

> Le filtre couverture ≥80% est inutile : toutes les stations ≥2 ans le passent déjà. Critères retenus : **≥ 730 jours + dernière mesure ≥ 2024-01-01**.

### Distribution de durée (piézo éligibles)

| Bucket | Stations | % |
|--------|----------|---|
| 2-3 ans | 65 | 2% |
| 3-10 ans | 532 | 18% |
| 10-20 ans | 1 131 | 39% |
| 20-40 ans | 1 168 | 40% |
| 40+ ans | 39 | 1% |

78% des stations ont 10+ ans de données → embeddings riches.

### Volumétrie embeddings (fenêtres 365j, stride 90j)

| | Piézo | Hydro | Total |
|--|-------|-------|-------|
| Stations | 2 935 | 2 535 | 5 470 |
| Fenêtres totales | 208 066 | 161 323 | **369 389** |
| Moy. fenêtres/station | 71 | 64 | |
| Min / Max fenêtres | 5 / 220 | 5 / 230 | |
| **Stockage estimé** | ~255 MB | ~195 MB | **~450 MB** |

Calcul : 369K × 320 × 4 bytes + index HNSW (~30% overhead) ≈ 580 MB. Négligeable sur une base de 76 GB.

---

## 2. Architecture

### Ce qu'on veut

4 tables dans un schema `ml` dédié :

```
ml.piezo_station_embeddings     2 935 lignes     ← 1 embedding/station (clustering, similarité)
ml.piezo_window_embeddings    208 066 lignes     ← 1 embedding/fenêtre (trajectoires, régimes)
ml.hydro_station_embeddings     2 535 lignes     ← idem hydro
ml.hydro_window_embeddings    161 323 lignes     ← idem hydro
```

Deux modèles TS2Vec séparés (variables et dynamiques différentes) :
- **Piézo** : `niveau_nappe_eau` + `temperature_2m` + `total_precipitation` + `potential_evaporation`
- **Hydro** : `resultat_obs_elab` (QmnJ) + `temperature_2m` + `total_precipitation` + `potential_evaporation`

### Mise à jour nightly

**Training** : manuel (ou mensuel). Coûteux (~30min GPU). Produit `model.pt` + `scaler.pkl`.

**Nightly encode** : sensor-driven après les domain pipelines. Rapide (~2min GPU). Ne ré-encode que la **dernière fenêtre** de chaque station active (les fenêtres historiques ne bougent pas). Met à jour l'embedding station (mean pooling recalculé).

### Chaîne de sensors

```
Bronze → shared_staging → piezo_daily ──→ dimensions
                                     ╰──→ ml_piezo_embeddings → ml_piezo_clusters    [NOUVEAU]
                        → hydro_daily ──→ dimensions
                                     ╰──→ ml_hydro_embeddings → ml_hydro_clusters    [NOUVEAU]
```

Embeddings et dimensions tournent en parallèle. Pas de dépendance entre eux.

---

## 3. TS2Vec

Apprentissage contrastif hiérarchique (Yue et al., AAAI 2022). Self-supervised.

### Hyperparamètres

| Paramètre | Valeur | Justification |
|-----------|--------|---------------|
| `embedding_dim` | 320 | 5 470 stations → largement suffisant |
| `depth` | 10 | Champ réceptif 2^10 = 1024 jours (~3 ans) |
| `input_dims` | 4 | 4 variables par domaine |
| `window_size` | 365 jours | 1 cycle hydrologique complet |
| `stride` | 90 jours | ~4 fenêtres/an (saisonnier) |
| `n_epochs` | 200 | Early stopping patience=20 |
| `batch_size` | 16 | Adapté à 2 935 stations GPU |

### Vendorisation

TS2Vec n'est pas sur PyPI. On copie les ~5 fichiers sources dans `src/hubeau_pipeline/ml/ts2vec/` :
- `ts2vec.py` — classe principale
- `encoder.py` — dilated CNN backbone
- `losses.py` — hierarchical contrastive loss
- `utils.py` — padding, split
- `__init__.py`

Source : [github.com/yuezhihan/ts2vec](https://github.com/yuezhihan/ts2vec)

---

## 4. Schéma PostgreSQL

### Extension et schema

Ajout dans `docker/postgres/init.sql` :

```sql
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS ml;
```

> pgvector 0.8.1 est déjà disponible dans `timescale/timescaledb-ha:pg16`. Supporte HNSW.

### Tables

```sql
-- ======================================================================
-- PIÉZOMÉTRIE
-- ======================================================================

CREATE TABLE ml.piezo_station_embeddings (
    code_bss        TEXT PRIMARY KEY,
    embedding       vector(320) NOT NULL,
    cluster_id      INT,
    model_version   TEXT NOT NULL,
    n_days          INT NOT NULL,           -- jours de données utilisés
    n_windows       INT NOT NULL,           -- fenêtres agrégées
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

-- ======================================================================
-- HYDROMÉTRIE
-- ======================================================================

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

-- ======================================================================
-- INDEX HNSW (meilleur que IVFFlat pour < 1M vecteurs)
-- ======================================================================

CREATE INDEX idx_piezo_station_emb_hnsw
    ON ml.piezo_station_embeddings
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

CREATE INDEX idx_hydro_station_emb_hnsw
    ON ml.hydro_station_embeddings
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

-- Pas d'index HNSW sur les fenêtres (query par station_id, pas par similarité)
CREATE INDEX idx_piezo_window_station ON ml.piezo_window_embeddings (code_bss, window_start);
CREATE INDEX idx_hydro_window_station ON ml.hydro_window_embeddings (code_station, window_start);
```

> **HNSW vs IVFFlat** : HNSW est meilleur en recall et ne nécessite pas de `VACUUM` pour maintenir l'index. Pour 5K vecteurs, m=16 ef_construction=64 est optimal.

---

## 5. Structure des fichiers

```
src/hubeau_pipeline/
├── ml/
│   ├── __init__.py
│   ├── ts2vec/                       # Vendorisé
│   │   ├── __init__.py
│   │   ├── ts2vec.py
│   │   ├── encoder.py
│   │   ├── losses.py
│   │   └── utils.py
│   └── latent_space/
│       ├── __init__.py
│       ├── encoder.py                # TS2VecEncoder wrapper
│       ├── data.py                   # Chargement depuis Gold tables
│       ├── persistence.py            # pgvector CRUD (upsert, search)
│       └── clustering.py             # HDBSCAN + métriques
├── assets/
│   └── ml_assets.py                  # 6 assets Dagster
├── jobs/
│   └── ml_jobs.py                    # 4 jobs
```

---

## 6. Composants Python

### 6.1 encoder.py — TS2VecEncoder

```python
import torch
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple, Optional

class TS2VecEncoder:
    """Wrapper TS2Vec pour séries hydrologiques multivariate."""

    def __init__(
        self,
        input_dims: int = 4,
        embedding_dim: int = 320,
        hidden_dim: int = 320,
        depth: int = 10,
        device: str = "auto",
    ):
        self.device = self._resolve_device(device)
        self.model = None  # Lazy init
        self.input_dims = input_dims
        self.embedding_dim = embedding_dim
        self.hidden_dim = hidden_dim
        self.depth = depth

    @staticmethod
    def _resolve_device(device: str) -> str:
        if device == "auto":
            return "cuda" if torch.cuda.is_available() else "cpu"
        return device

    def fit(
        self,
        train_series: List[np.ndarray],   # [(T_i, 4)] par station
        n_epochs: int = 200,
        lr: float = 1e-3,
        batch_size: int = 16,
        early_stopping_patience: int = 20,
    ) -> "TS2VecEncoder":
        """Entraîne TS2Vec sur une liste de séries multivariate."""
        from ..ts2vec import TS2Vec

        self.model = TS2Vec(
            input_dims=self.input_dims,
            output_dims=self.embedding_dim,
            hidden_dims=self.hidden_dim,
            depth=self.depth,
            device=self.device,
        )
        # TS2Vec attend un array 3D (N, T, C) avec padding
        # On utilise le mode "irregular" qui gère des longueurs différentes
        self.model.fit(
            train_series,
            n_epochs=n_epochs,
            lr=lr,
            batch_size=batch_size,
        )
        return self

    def encode_windows(
        self,
        series: np.ndarray,               # (T, 4) une station
        window_size: int = 365,
        stride: int = 90,
        dates: Optional[List] = None,
    ) -> Tuple[np.ndarray, List[Tuple[str, str]]]:
        """
        Encode une série en fenêtres glissantes.
        Retourne (n_windows, 320) et [(start_date, end_date), ...].
        """
        T = len(series)
        embeddings = []
        window_dates = []

        for start in range(0, T - window_size + 1, stride):
            end = start + window_size
            window = series[start:end]  # (365, 4)
            emb = self.model.encode(window[np.newaxis], encoding_window="full_series")
            embeddings.append(emb.squeeze())  # (320,)
            if dates:
                window_dates.append((str(dates[start]), str(dates[end - 1])))

        return np.stack(embeddings), window_dates

    def encode_full(self, series: np.ndarray) -> np.ndarray:
        """Encode une série complète → (320,). Pour embedding station."""
        emb = self.model.encode(series[np.newaxis], encoding_window="full_series")
        return emb.squeeze()  # (320,)

    def save(self, path: Path) -> None:
        self.model.save(str(path))

    @classmethod
    def load(cls, path: Path, device: str = "auto") -> "TS2VecEncoder":
        from ..ts2vec import TS2Vec
        enc = cls.__new__(cls)
        enc.device = cls._resolve_device(device)
        enc.model = TS2Vec.load(str(path))
        return enc
```

### 6.2 data.py — Chargement depuis Gold

```python
import numpy as np
import pandas as pd
from typing import Dict

def load_piezo_series(pg, min_days: int = 730) -> Dict[str, np.ndarray]:
    """
    Charge les séries piézo éligibles depuis Gold.
    Retourne {code_bss: (T, 4)} trié par date, NaN interpolés.
    """
    with pg.get_connection() as conn:
        df = pd.read_sql("""
            SELECT code_bss, date,
                   niveau_nappe_eau, temperature_2m,
                   total_precipitation, potential_evaporation
            FROM gold.hubeau_daily_chroniques
            WHERE code_bss IN (
                SELECT code_bss
                FROM gold.hubeau_daily_chroniques
                GROUP BY code_bss
                HAVING COUNT(*) >= %(min_days)s
                   AND MAX(date) >= '2024-01-01'
            )
            ORDER BY code_bss, date
        """, conn, params={"min_days": min_days})

    cols = ["niveau_nappe_eau", "temperature_2m",
            "total_precipitation", "potential_evaporation"]
    result = {}
    for code_bss, group in df.groupby("code_bss"):
        arr = group[cols].interpolate().fillna(0).values.astype(np.float32)
        result[code_bss] = arr
    return result


def load_hydro_series(pg, min_days: int = 730) -> Dict[str, np.ndarray]:
    """
    Charge les séries hydro éligibles (QmnJ) depuis Gold.
    Retourne {code_station: (T, 4)}.
    """
    with pg.get_connection() as conn:
        df = pd.read_sql("""
            SELECT code_station, date,
                   resultat_obs_elab, temperature_2m,
                   total_precipitation, potential_evaporation
            FROM gold.hydro_daily_chroniques
            WHERE grandeur_hydro_elab = 'QmnJ'
              AND code_station IN (
                SELECT code_station
                FROM gold.hydro_daily_chroniques
                WHERE grandeur_hydro_elab = 'QmnJ'
                GROUP BY code_station
                HAVING COUNT(*) >= %(min_days)s
                   AND MAX(date) >= '2024-01-01'
            )
            ORDER BY code_station, date
        """, conn, params={"min_days": min_days})

    cols = ["resultat_obs_elab", "temperature_2m",
            "total_precipitation", "potential_evaporation"]
    result = {}
    for code_station, group in df.groupby("code_station"):
        arr = group[cols].interpolate().fillna(0).values.astype(np.float32)
        result[code_station] = arr
    return result


def load_piezo_dates(pg, min_days: int = 730) -> Dict[str, list]:
    """Charge les dates pour les fenêtres (nécessaire pour window_start/end)."""
    with pg.get_connection() as conn:
        df = pd.read_sql("""
            SELECT code_bss, date
            FROM gold.hubeau_daily_chroniques
            WHERE code_bss IN (
                SELECT code_bss
                FROM gold.hubeau_daily_chroniques
                GROUP BY code_bss
                HAVING COUNT(*) >= %(min_days)s
                   AND MAX(date) >= '2024-01-01'
            )
            ORDER BY code_bss, date
        """, conn, params={"min_days": min_days})

    return {bss: grp["date"].tolist() for bss, grp in df.groupby("code_bss")}


def load_hydro_dates(pg, min_days: int = 730) -> Dict[str, list]:
    """Idem pour hydro."""
    with pg.get_connection() as conn:
        df = pd.read_sql("""
            SELECT code_station, date
            FROM gold.hydro_daily_chroniques
            WHERE grandeur_hydro_elab = 'QmnJ'
              AND code_station IN (
                SELECT code_station
                FROM gold.hydro_daily_chroniques
                WHERE grandeur_hydro_elab = 'QmnJ'
                GROUP BY code_station
                HAVING COUNT(*) >= %(min_days)s
                   AND MAX(date) >= '2024-01-01'
            )
            ORDER BY code_station, date
        """, conn, params={"min_days": min_days})

    return {st: grp["date"].tolist() for st, grp in df.groupby("code_station")}
```

### 6.3 persistence.py — pgvector CRUD

```python
import numpy as np
from typing import Dict, List, Tuple
from pgvector.psycopg2 import register_vector


def init_ml_schema(pg):
    """Crée le schema ml + tables + index si inexistants."""
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute("CREATE SCHEMA IF NOT EXISTS ml")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ml.piezo_station_embeddings (
                code_bss TEXT PRIMARY KEY, embedding vector(320) NOT NULL,
                cluster_id INT, model_version TEXT NOT NULL,
                n_days INT NOT NULL, n_windows INT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ml.piezo_window_embeddings (
                code_bss TEXT NOT NULL, window_start DATE NOT NULL,
                window_end DATE NOT NULL, embedding vector(320) NOT NULL,
                model_version TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (code_bss, window_start)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ml.hydro_station_embeddings (
                code_station TEXT PRIMARY KEY, embedding vector(320) NOT NULL,
                cluster_id INT, model_version TEXT NOT NULL,
                n_days INT NOT NULL, n_windows INT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ml.hydro_window_embeddings (
                code_station TEXT NOT NULL, window_start DATE NOT NULL,
                window_end DATE NOT NULL, embedding vector(320) NOT NULL,
                model_version TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (code_station, window_start)
            )
        """)
        conn.commit()


def upsert_piezo_embeddings(
    pg,
    station_embeddings: Dict[str, np.ndarray],
    window_embeddings: Dict[str, Tuple[np.ndarray, List[Tuple[str, str]]]],
    version: str,
):
    """
    Upsert station + window embeddings piézo.
    station_embeddings: {code_bss: (320,)}
    window_embeddings: {code_bss: ((n_windows, 320), [(start, end), ...])}
    """
    with pg.get_connection() as conn:
        register_vector(conn)
        cur = conn.cursor()

        for bss, emb in station_embeddings.items():
            wins = window_embeddings.get(bss)
            n_windows = wins[0].shape[0] if wins else 0
            cur.execute("""
                INSERT INTO ml.piezo_station_embeddings
                    (code_bss, embedding, model_version, n_days, n_windows, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (code_bss) DO UPDATE SET
                    embedding = EXCLUDED.embedding,
                    model_version = EXCLUDED.model_version,
                    n_days = EXCLUDED.n_days,
                    n_windows = EXCLUDED.n_windows,
                    updated_at = NOW()
            """, (bss, emb, version, 0, n_windows))

        for bss, (embs, dates) in window_embeddings.items():
            for emb, (start, end) in zip(embs, dates):
                cur.execute("""
                    INSERT INTO ml.piezo_window_embeddings
                        (code_bss, window_start, window_end, embedding, model_version)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (code_bss, window_start) DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        window_end = EXCLUDED.window_end,
                        model_version = EXCLUDED.model_version
                """, (bss, start, end, emb, version))

        conn.commit()


def upsert_hydro_embeddings(
    pg,
    station_embeddings: Dict[str, np.ndarray],
    window_embeddings: Dict[str, Tuple[np.ndarray, List[Tuple[str, str]]]],
    version: str,
):
    """Idem pour hydro."""
    with pg.get_connection() as conn:
        register_vector(conn)
        cur = conn.cursor()

        for station, emb in station_embeddings.items():
            wins = window_embeddings.get(station)
            n_windows = wins[0].shape[0] if wins else 0
            cur.execute("""
                INSERT INTO ml.hydro_station_embeddings
                    (code_station, embedding, model_version, n_days, n_windows, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (code_station) DO UPDATE SET
                    embedding = EXCLUDED.embedding,
                    model_version = EXCLUDED.model_version,
                    n_days = EXCLUDED.n_days,
                    n_windows = EXCLUDED.n_windows,
                    updated_at = NOW()
            """, (station, emb, version, 0, n_windows))

        for station, (embs, dates) in window_embeddings.items():
            for emb, (start, end) in zip(embs, dates):
                cur.execute("""
                    INSERT INTO ml.hydro_window_embeddings
                        (code_station, window_start, window_end, embedding, model_version)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (code_station, window_start) DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        window_end = EXCLUDED.window_end,
                        model_version = EXCLUDED.model_version
                """, (station, start, end, emb, version))

        conn.commit()


def search_similar_piezo(pg, code_bss: str, k: int = 10) -> list:
    """Recherche les k stations piézo les plus similaires (cosine)."""
    with pg.get_connection() as conn:
        register_vector(conn)
        cur = conn.cursor()
        cur.execute("""
            SELECT code_bss, embedding <=> (
                SELECT embedding FROM ml.piezo_station_embeddings WHERE code_bss = %s
            ) AS distance
            FROM ml.piezo_station_embeddings
            WHERE code_bss != %s
            ORDER BY distance LIMIT %s
        """, (code_bss, code_bss, k))
        return [{"code_bss": r[0], "distance": float(r[1])} for r in cur.fetchall()]
```

### 6.4 clustering.py

```python
import numpy as np
import hdbscan
from sklearn.metrics import silhouette_score, davies_bouldin_score
from pgvector.psycopg2 import register_vector
from typing import Dict


def cluster_and_update(pg, table: str, id_col: str, min_cluster_size: int = 5) -> Dict:
    """
    Charge les embeddings station, HDBSCAN, UPDATE cluster_id.
    table: "ml.piezo_station_embeddings" ou "ml.hydro_station_embeddings"
    """
    with pg.get_connection() as conn:
        register_vector(conn)
        cur = conn.cursor()
        cur.execute(f"SELECT {id_col}, embedding FROM {table}")
        rows = cur.fetchall()

    if not rows:
        return {"n_clusters": 0, "silhouette_score": -1, "davies_bouldin_index": -1}

    ids = [r[0] for r in rows]
    embs = np.stack([np.array(r[1]) for r in rows])

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=3,
        metric="euclidean",
    )
    labels = clusterer.fit_predict(embs)

    # Métriques (exclure noise label=-1)
    mask = labels >= 0
    n_clustered = mask.sum()
    n_clusters = len(set(labels[mask]))

    sil = silhouette_score(embs[mask], labels[mask]) if n_clusters >= 2 else -1
    db = davies_bouldin_score(embs[mask], labels[mask]) if n_clusters >= 2 else -1

    # Écrire les labels
    with pg.get_connection() as conn:
        cur = conn.cursor()
        for sid, label in zip(ids, labels):
            cur.execute(
                f"UPDATE {table} SET cluster_id = %s WHERE {id_col} = %s",
                (int(label), sid),
            )
        conn.commit()

    return {
        "n_clusters": n_clusters,
        "n_noise": int((labels == -1).sum()),
        "silhouette_score": float(sil),
        "davies_bouldin_index": float(db),
    }
```

---

## 7. Assets Dagster

### ml_assets.py

```python
from dagster import asset, AssetExecutionContext, MetadataValue
from ..resources import PostgreSQLResource
from pathlib import Path
import numpy as np
import json, joblib
from datetime import datetime
from sklearn.preprocessing import StandardScaler

MODELS_DIR = Path("/var/ml/models")


# ======================================================================
# TRAINING — Manuel, GPU (~30min)
# ======================================================================

@asset(
    group_name="ml_piezo",
    deps=["hubeau_daily_chroniques"],
    description="Entraîne TS2Vec piézométrie (2 935 stations, GPU)",
)
def ml_piezo_model_train(context: AssetExecutionContext, pg: PostgreSQLResource):
    from ..ml.latent_space.encoder import TS2VecEncoder
    from ..ml.latent_space.data import load_piezo_series

    series_dict = load_piezo_series(pg, min_days=730)
    context.log.info(f"{len(series_dict)} stations éligibles")

    # Normalisation globale
    all_data = np.concatenate(list(series_dict.values()))
    scaler = StandardScaler().fit(all_data)
    scaled = [scaler.transform(arr) for arr in series_dict.values()]

    # Train
    encoder = TS2VecEncoder(input_dims=4, embedding_dim=320, depth=10)
    encoder.fit(scaled, n_epochs=200, lr=1e-3, batch_size=16)

    # Save
    version = f"piezo_{datetime.now():%Y%m%d_%H%M}"
    path = MODELS_DIR / version
    path.mkdir(parents=True, exist_ok=True)
    encoder.save(path / "model.pt")
    joblib.dump(scaler, path / "scaler.pkl")
    json.dump(list(series_dict.keys()), (path / "stations.json").open("w"))
    (MODELS_DIR / "piezo_latest").write_text(version)

    context.add_output_metadata({
        "model_version": version,
        "n_stations": len(series_dict),
        "device": encoder.device,
    })


@asset(
    group_name="ml_hydro",
    deps=["hydro_daily_chroniques"],
    description="Entraîne TS2Vec hydrométrie (2 535 stations, GPU)",
)
def ml_hydro_model_train(context: AssetExecutionContext, pg: PostgreSQLResource):
    from ..ml.latent_space.encoder import TS2VecEncoder
    from ..ml.latent_space.data import load_hydro_series

    series_dict = load_hydro_series(pg, min_days=730)
    context.log.info(f"{len(series_dict)} stations éligibles")

    all_data = np.concatenate(list(series_dict.values()))
    scaler = StandardScaler().fit(all_data)
    scaled = [scaler.transform(arr) for arr in series_dict.values()]

    encoder = TS2VecEncoder(input_dims=4, embedding_dim=320, depth=10)
    encoder.fit(scaled, n_epochs=200, lr=1e-3, batch_size=16)

    version = f"hydro_{datetime.now():%Y%m%d_%H%M}"
    path = MODELS_DIR / version
    path.mkdir(parents=True, exist_ok=True)
    encoder.save(path / "model.pt")
    joblib.dump(scaler, path / "scaler.pkl")
    json.dump(list(series_dict.keys()), (path / "stations.json").open("w"))
    (MODELS_DIR / "hydro_latest").write_text(version)

    context.add_output_metadata({
        "model_version": version,
        "n_stations": len(series_dict),
        "device": encoder.device,
    })


# ======================================================================
# NIGHTLY ENCODE — Sensor-driven, GPU (~2min)
# ======================================================================

@asset(
    group_name="ml_piezo",
    deps=["hubeau_daily_chroniques"],
    description="Nightly: encode piézomètres → 208K fenêtres + 2 935 stations",
)
def ml_piezo_embeddings_update(context: AssetExecutionContext, pg: PostgreSQLResource):
    from ..ml.latent_space.encoder import TS2VecEncoder
    from ..ml.latent_space.data import load_piezo_series, load_piezo_dates
    from ..ml.latent_space.persistence import init_ml_schema, upsert_piezo_embeddings

    version = (MODELS_DIR / "piezo_latest").read_text().strip()
    path = MODELS_DIR / version
    encoder = TS2VecEncoder.load(path / "model.pt")
    scaler = joblib.load(path / "scaler.pkl")

    init_ml_schema(pg)

    series_dict = load_piezo_series(pg, min_days=730)
    dates_dict = load_piezo_dates(pg, min_days=730)

    station_embeddings = {}
    window_embeddings = {}

    for bss, arr in series_dict.items():
        scaled = scaler.transform(arr)
        dates = dates_dict.get(bss, [])

        # Fenêtres glissantes
        win_embs, win_dates = encoder.encode_windows(
            scaled, window_size=365, stride=90, dates=dates
        )
        window_embeddings[bss] = (win_embs, win_dates)

        # Embedding station = mean pooling des fenêtres
        station_embeddings[bss] = win_embs.mean(axis=0)

    upsert_piezo_embeddings(pg, station_embeddings, window_embeddings, version)

    context.add_output_metadata({
        "n_stations": len(station_embeddings),
        "n_windows": sum(w[0].shape[0] for w in window_embeddings.values()),
        "model_version": version,
    })


@asset(
    group_name="ml_hydro",
    deps=["hydro_daily_chroniques"],
    description="Nightly: encode hydro → 161K fenêtres + 2 535 stations",
)
def ml_hydro_embeddings_update(context: AssetExecutionContext, pg: PostgreSQLResource):
    from ..ml.latent_space.encoder import TS2VecEncoder
    from ..ml.latent_space.data import load_hydro_series, load_hydro_dates
    from ..ml.latent_space.persistence import init_ml_schema, upsert_hydro_embeddings

    version = (MODELS_DIR / "hydro_latest").read_text().strip()
    path = MODELS_DIR / version
    encoder = TS2VecEncoder.load(path / "model.pt")
    scaler = joblib.load(path / "scaler.pkl")

    init_ml_schema(pg)

    series_dict = load_hydro_series(pg, min_days=730)
    dates_dict = load_hydro_dates(pg, min_days=730)

    station_embeddings = {}
    window_embeddings = {}

    for station, arr in series_dict.items():
        scaled = scaler.transform(arr)
        dates = dates_dict.get(station, [])

        win_embs, win_dates = encoder.encode_windows(
            scaled, window_size=365, stride=90, dates=dates
        )
        window_embeddings[station] = (win_embs, win_dates)
        station_embeddings[station] = win_embs.mean(axis=0)

    upsert_hydro_embeddings(pg, station_embeddings, window_embeddings, version)

    context.add_output_metadata({
        "n_stations": len(station_embeddings),
        "n_windows": sum(w[0].shape[0] for w in window_embeddings.values()),
        "model_version": version,
    })


# ======================================================================
# CLUSTERING — Après encode
# ======================================================================

@asset(
    group_name="ml_piezo",
    deps=["ml_piezo_embeddings_update"],
    description="HDBSCAN clustering piézomètres (2 935 stations)",
)
def ml_piezo_clusters(context: AssetExecutionContext, pg: PostgreSQLResource):
    from ..ml.latent_space.clustering import cluster_and_update
    result = cluster_and_update(pg, "ml.piezo_station_embeddings", "code_bss")
    context.add_output_metadata({
        "n_clusters": result["n_clusters"],
        "n_noise": result["n_noise"],
        "silhouette": MetadataValue.float(result["silhouette_score"]),
        "davies_bouldin": MetadataValue.float(result["davies_bouldin_index"]),
    })


@asset(
    group_name="ml_hydro",
    deps=["ml_hydro_embeddings_update"],
    description="HDBSCAN clustering hydrométrie (2 535 stations)",
)
def ml_hydro_clusters(context: AssetExecutionContext, pg: PostgreSQLResource):
    from ..ml.latent_space.clustering import cluster_and_update
    result = cluster_and_update(pg, "ml.hydro_station_embeddings", "code_station")
    context.add_output_metadata({
        "n_clusters": result["n_clusters"],
        "n_noise": result["n_noise"],
        "silhouette": MetadataValue.float(result["silhouette_score"]),
        "davies_bouldin": MetadataValue.float(result["davies_bouldin_index"]),
    })
```

---

## 8. Jobs

### ml_jobs.py

```python
from dagster import define_asset_job, AssetSelection

# Training (manuel)
ml_piezo_train_job = define_asset_job(
    name="ml_piezo_train_job",
    selection=AssetSelection.assets("ml_piezo_model_train"),
    description="Entraîne TS2Vec piézométrie (GPU, ~30min)",
)
ml_hydro_train_job = define_asset_job(
    name="ml_hydro_train_job",
    selection=AssetSelection.assets("ml_hydro_model_train"),
    description="Entraîne TS2Vec hydrométrie (GPU, ~30min)",
)

# Nightly encode + cluster (sensor-driven)
ml_piezo_embeddings_job = define_asset_job(
    name="ml_piezo_embeddings_job",
    selection=AssetSelection.assets("ml_piezo_embeddings_update", "ml_piezo_clusters"),
    description="Encode + cluster piézo (GPU, ~2min)",
)
ml_hydro_embeddings_job = define_asset_job(
    name="ml_hydro_embeddings_job",
    selection=AssetSelection.assets("ml_hydro_embeddings_update", "ml_hydro_clusters"),
    description="Encode + cluster hydro (GPU, ~2min)",
)
```

---

## 9. Sensor — Step 4 : Domain → Embeddings

Ajout dans `sensors.py` :

```python
from .jobs import ml_piezo_embeddings_job, ml_hydro_embeddings_job

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dbt_piezo_pipeline_daily_job, dbt_hydro_pipeline_daily_job],
    request_jobs=[ml_piezo_embeddings_job, ml_hydro_embeddings_job],
    default_status=DEFAULT_SENSOR_STATUS,
    minimum_interval_seconds=30,
    description="Step 4: Domain pipeline done → update embeddings (GPU)",
)
def domain_to_embeddings_sensor(context: RunStatusSensorContext):
    """
    Fires after each domain pipeline succeeds.
    Piezo done → piezo embeddings. Hydro done → hydro embeddings.
    Indépendant de la chaîne dimensions (parallèle).
    """
    completed_job = context.dagster_run.job_name
    run_id = context.dagster_run.run_id

    if completed_job == dbt_piezo_pipeline_daily_job.name:
        yield RunRequest(
            run_key=f"piezo_emb_{run_id}",
            job_name=ml_piezo_embeddings_job.name,
            tags={
                "trigger": "sensor",
                "sensor_name": "domain_to_embeddings_sensor",
                "parent_run_id": run_id,
                "pipeline_chain": "step_4_piezo_embeddings",
            },
        )
    elif completed_job == dbt_hydro_pipeline_daily_job.name:
        yield RunRequest(
            run_key=f"hydro_emb_{run_id}",
            job_name=ml_hydro_embeddings_job.name,
            tags={
                "trigger": "sensor",
                "sensor_name": "domain_to_embeddings_sensor",
                "parent_run_id": run_id,
                "pipeline_chain": "step_4_hydro_embeddings",
            },
        )
```

### Chaîne nightly complète

```
3h00  ERA5 Smart Update
4h00  Bronze piézo + hydro
      │
      ▼ (sensor step 1)
5h00  Shared staging (ERA5)
      │
      ▼ (sensor step 2)
5h30  ┌─ Piezo domain pipeline ──→ Piezo embeddings + clusters  [GPU]
      └─ Hydro domain pipeline ──→ Hydro embeddings + clusters  [GPU]
      │
      ▼ (sensor step 3, après les 2 domains)
7h00  Shared dimensions
```

---

## 10. GPU Docker

### Prérequis host

```bash
# NVIDIA Container Toolkit (RHEL 9 / Rocky 9)
dnf install -y nvidia-container-toolkit
nvidia-ctk runtime configure --runtime=docker
systemctl restart docker

# Vérifier
docker run --rm --gpus all nvidia/cuda:12.1-base nvidia-smi
```

### docker-compose.yml

```yaml
services:
  dlt_worker:
    # ... existant ...
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
    environment:
      # ... existant ...
      NVIDIA_VISIBLE_DEVICES: all
```

### docker/worker/Dockerfile

```dockerfile
# PyTorch avec CUDA 12.1
RUN pip install torch --index-url https://download.pytorch.org/whl/cu121
```

> Fallback CPU : si pas de GPU, `TS2VecEncoder._resolve_device("auto")` retourne `"cpu"`. Ça marche, c'est juste plus lent (~15min au lieu de ~2min pour l'encoding nightly).

---

## 11. Dépendances

### pyproject.toml

```toml
[project]
dependencies = [
    # ... existantes ...
    # ML - Latent space
    "torch>=2.0.0",
    "pgvector>=0.3.0",
    "hdbscan>=0.8.33",
    "umap-learn>=0.5.0",         # Pour viz future (pas utilisé en v1 nightly)
    "scikit-learn>=1.3.0",
    "joblib>=1.3.0",
]
```

### docker/postgres/init.sql

Ajouter :
```sql
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS ml;
```

### scripts/init_volumes.sh

Ajouter :
```bash
docker volume create brgm_ml_models
```

### docker-compose.yml — Volume

```yaml
services:
  dlt_worker:
    volumes:
      # ... existants ...
      - brgm_ml_models:/var/ml/models

volumes:
  brgm_ml_models:
    external: true
```

---

## 12. Wiring definitions.py

```python
from .assets.ml_assets import (
    ml_piezo_model_train, ml_hydro_model_train,
    ml_piezo_embeddings_update, ml_hydro_embeddings_update,
    ml_piezo_clusters, ml_hydro_clusters,
)
from .jobs.ml_jobs import (
    ml_piezo_train_job, ml_hydro_train_job,
    ml_piezo_embeddings_job, ml_hydro_embeddings_job,
)

defs = Definitions(
    assets=[
        *all_assets,
        ml_piezo_model_train, ml_hydro_model_train,
        ml_piezo_embeddings_update, ml_hydro_embeddings_update,
        ml_piezo_clusters, ml_hydro_clusters,
    ],
    jobs=[
        *all_jobs,
        ml_piezo_train_job, ml_hydro_train_job,
        ml_piezo_embeddings_job, ml_hydro_embeddings_job,
    ],
    sensors=[
        *all_sensors,
        domain_to_embeddings_sensor,
    ],
    # resources inchangés
)
```

---

## 13. Requêtes Superset

### Carte des clusters piézo

```sql
SELECT e.code_bss, e.cluster_id,
       d.station_latitude, d.station_longitude,
       d.nom_commune, d.nature_eh, d.milieu_eh
FROM ml.piezo_station_embeddings e
JOIN gold.dim_piezo_stations d ON e.code_bss = d.code_bss
WHERE e.cluster_id >= 0;  -- exclure noise (-1)
```

### 10 stations les plus similaires

```sql
SELECT s.code_bss, s.cluster_id,
       d.nom_commune, d.nature_eh,
       s.embedding <=> (
           SELECT embedding FROM ml.piezo_station_embeddings WHERE code_bss = 'BSS001'
       ) AS distance
FROM ml.piezo_station_embeddings s
JOIN gold.dim_piezo_stations d ON s.code_bss = d.code_bss
WHERE s.code_bss != 'BSS001'
ORDER BY distance LIMIT 10;
```

### Trajectoire temporelle d'une station

```sql
SELECT window_start, window_end, embedding
FROM ml.piezo_window_embeddings
WHERE code_bss = 'BSS001'
ORDER BY window_start;
```

### Enrichissement forecasting (static covariates)

```sql
SELECT c.code_bss, c.date, c.niveau_nappe_eau,
       e.embedding
FROM gold.hubeau_daily_chroniques c
JOIN ml.piezo_station_embeddings e ON c.code_bss = e.code_bss
WHERE c.code_bss = 'BSS001';
```

---

## 14. Plan d'implémentation

| Phase | Tâches | Dépendances |
|-------|--------|-------------|
| **P0** | Vendoriser TS2Vec dans `ml/ts2vec/` | Aucune |
| **P1** | `encoder.py` (wrapper TS2Vec) | P0 |
| **P2** | `data.py` (chargement Gold) | Aucune |
| **P3** | `persistence.py` (pgvector CRUD) + `init.sql` | Aucune |
| **P4** | `clustering.py` (HDBSCAN) | Aucune |
| **P5** | `ml_assets.py` (6 assets) + `ml_jobs.py` (4 jobs) | P1-P4 |
| **P6** | Docker : GPU worker + `pyproject.toml` + volume | P0 |
| **P7** | Sensor `domain_to_embeddings_sensor` + `definitions.py` | P5 |
| **P8** | Test E2E : train manuel → nightly → vérif tables `ml.*` | P6-P7 |

```
P0 → P1 ─┐
P2 ───────┤
P3 ───────┼→ P5 → P7 → P8
P4 ───────┘        ↑
P6 ────────────────┘
```

P0-P4 et P6 sont tous parallélisables. Le chemin critique est P0 → P1 → P5 → P7 → P8.

---

## 15. Risques

| Risque | Mitigation |
|--------|------------|
| Pas de GPU sur le serveur | `device="auto"` fallback CPU (~15min nightly au lieu de 2min) |
| pgvector 0.8.1 pas compilé dans l'image | Déjà disponible (`pg_available_extensions` vérifié) |
| OOM chargement 23M lignes piézo | Requête SQL avec sous-requête éligibilité, pandas groupby |
| PyTorch CUDA image ~2GB | `cu121` minimal, ou CPU-only si contrainte taille |
| Modèle perdu (container restart) | Volume externe `brgm_ml_models` |
| HDBSCAN instable dim 320 | Pré-réduction UMAP(50) si silhouette < 0.3 |
| `max_concurrent_runs: 1` bloque GPU job | Embedding job rapide (~2min), attente queue acceptable |
| Nightly re-encode tout (pas incrémental) | v2 : tracker les stations modifiées, skip les inchangées |

---

## 16. Métriques de succès

| Métrique | Seuil |
|----------|-------|
| Silhouette score HDBSCAN | > 0.4 |
| Cohérence avec `nature_eh`/`milieu_eh` | > 70% des clusters homogènes |
| Temps encoding nightly (GPU) | < 5 min |
| Temps encoding nightly (CPU fallback) | < 20 min |
| Temps kNN pgvector (HNSW) | < 50 ms |
| Taille tables `ml.*` | < 600 MB |
| Amélioration forecasting (static covariates) | > 5% RMSE vs baseline |
