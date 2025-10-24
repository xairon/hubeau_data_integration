# Analyse Gestion Mémoire - Hub'Eau Pipeline

## 🎯 Objectif

Optimiser à mort la gestion mémoire pour éviter OOM (Out Of Memory) sur VPS 3GB RAM.

## 🔍 Analyse de la Chaîne Actuelle

### 1. **hubeau_assets.py** - Streaming par Batch

**Lignes 326-362** :
```python
batch = []  # ← Liste en RAM
for record in source:
    batch.append(record)  # ← Accumulation en RAM

    if len(batch) >= BATCH_SIZE:
        postgres_bulk_destination.load_batch(...)  # ← Chargement
        batch = []  # ← Vider
        gc.collect()  # ← GC agressif
```

**Problèmes** :
1. ❌ **`batch = []`** : Liste Python = Overhead mémoire (~1KB/record avec dict)
2. ❌ **Accumulation** : BATCH_SIZE records en RAM simultanément
3. ❌ **GC manuel** : gc.collect() aide mais pas instantané
4. ❌ **Copies multiples** : record dict → list → DataFrame → StringIO → COPY

### 2. **postgres_optimized_v2.py** - load_batch()

**Lignes 439-472** :
```python
def load_batch(table_name, data: List[Dict], write_disposition, ...):
    df = pd.DataFrame(data)  # ← COPIE 1: List[Dict] → DataFrame

    if write_disposition == "replace":
        self._truncate_cascade(table_name)
        self._copy_from_dataframe(df, table_name)  # ← COPIE 2
    elif write_disposition == "merge":
        self._upsert_dataframe(df, table_name, primary_keys)  # ← COPIE 3
    ...
```

**Problèmes** :
1. ❌ **`pd.DataFrame(data)`** : Copie complète des données
2. ❌ **Pas de streaming** : Tout le batch doit tenir en RAM
3. ❌ **Copies multiples** : List → DataFrame → cleaning → CSV → COPY

### 3. **_clean_dataframe_inplace()** - "Inplace" Qui Ne L'Est Pas Toujours

**Lignes 172-295** :
```python
def _clean_dataframe_inplace(df, table_name, conn):
    # Cast types
    df[col] = pd.to_numeric(df[col], errors='coerce')  # ← NOUVELLE SÉRIE
    df[col] = df[col].fillna(pd.NA)  # ← NOUVELLE SÉRIE
    df[col] = df[col].astype('Int64')  # ← NOUVELLE SÉRIE

    # Clean objects
    original_col = df[col].copy()  # ← COPIE EXPLICITE (ligne 214)
    df.loc[~mask_none, col] = df.loc[~mask_none, col].apply(extract_first)  # ← NOUVELLE SÉRIE
```

**Problèmes** :
1. ❌ **Copie "original_col"** : Ligne 214 fait une copie explicite
2. ❌ **Chaining** : Chaque opération Pandas crée une nouvelle série
3. ❌ **`.apply()`** : Pas vectorisé, lent et consomme RAM

### 4. **_copy_from_dataframe()** - Buffer StringIO

**Lignes 341-398** :
```python
def _copy_from_dataframe(df, table_name, conn):
    # Nettoyer
    df = self._clean_dataframe_inplace(df, table_name, conn)  # ← COPIES

    # COPY avec buffer
    output = io.StringIO()  # ← Buffer en RAM
    df.to_csv(output, sep='\t', ...)  # ← COPIE 4: DataFrame → CSV en RAM
    output.seek(0)

    cursor.copy_expert(copy_sql, output)  # ← COPIE 5: StringIO → PostgreSQL
```

**Problèmes** :
1. ❌ **StringIO** : Buffer CSV complet en RAM
2. ❌ **df.to_csv()** : Crée représentation texte complète
3. ❌ **Pas de streaming** : Tout doit être en mémoire

### 5. **_upsert_dataframe()** - Staging Table

**Lignes 277-412** :
```python
def _upsert_dataframe(df, table_name, primary_keys):
    # Deduplicate
    df = df.drop_duplicates(subset=primary_keys)  # ← COPIE COMPLÈTE

    # CREATE staging
    cursor.execute(f"CREATE TEMP TABLE {staging_table} ...")

    # COPY vers staging
    output = io.StringIO()  # ← Buffer RAM
    df.to_csv(output, ...)  # ← COPIE
    cursor.copy_expert(...)

    # UPSERT staging → main
    cursor.execute(f"INSERT INTO ... FROM {staging_table} ...")
```

**Problèmes** :
1. ❌ **drop_duplicates()** : Copie complète du DataFrame
2. ❌ **Staging table** : Données dupliquées en PostgreSQL (staging + main)
3. ❌ **StringIO** : Idem _copy_from_dataframe

## 📊 Calcul Mémoire Actuel

### Exemple : Batch de 50,000 records

**Hypothèses** :
- 1 record = 30 colonnes × 20 chars = ~600 bytes brut
- Python dict overhead = +400 bytes
- **Total par record** : ~1 KB

**Mémoire utilisée (estimation)** :
```
1. batch list (hubeau_assets):       50k × 1KB = 50 MB
2. pd.DataFrame(batch):               50k × 1KB = 50 MB (COPIE)
3. _clean_dataframe copies:           50k × 1KB = 50 MB (COPIE)
4. StringIO buffer:                   50k × 600B = 30 MB (CSV)
5. PostgreSQL COPY buffer:            30 MB (transit)
---
TOTAL PEAK:                           ~210 MB par batch
```

**Avec ecoulement_observations @ 50k batch** :
- Batch 1 : 210 MB
- Batch 2 : 210 MB
- **PEAK** : ~420 MB (si GC lent)
- **+ source iterator** : +100-200 MB
- **+ Pandas overhead** : +100 MB
- **+ Python interpreter** : +200 MB
- **TOTAL** : **~1 GB par worker**

**Problème** : VPS 3GB RAM, multi-workers → OOM SIGKILL !

## 🚀 Solutions d'Optimisation

### Solution 1 : Zero-Copy Streaming (Idéal)

**Principe** : Stream records DIRECTEMENT vers PostgreSQL sans accumulation.

```python
# AVANT (mauvais)
batch = []
for record in source:
    batch.append(record)
    if len(batch) >= BATCH_SIZE:
        load_batch(batch)  # ← Copie List → DataFrame → CSV
        batch = []

# APRÈS (optimal)
def stream_to_postgres(source, table_name, batch_size):
    """Stream direct sans accumulation"""
    conn = get_connection()
    cursor = conn.cursor()

    # Créer pipe PostgreSQL COPY
    copy_sql = f"COPY {table_name} FROM STDIN WITH CSV"

    # Writer direct (pas de buffer intermédiaire)
    with cursor.copy(copy_sql) as copy_writer:
        buffer = []
        for record in source:
            buffer.append(record)

            if len(buffer) >= 1000:  # Mini-batch (1k)
                # Écrire direct au pipe (pas de StringIO)
                for row in buffer:
                    copy_writer.write_row(row.values())
                buffer = []  # Vider immédiatement
```

**Avantages** :
- ✅ Mémoire constante (~1-5 MB)
- ✅ Pas de copies
- ✅ Streaming pur

**Inconvénients** :
- Requiert psycopg3 ou custom COPY implementation
- Pas compatible avec cleaning/casting complexe

### Solution 2 : Batch Chunking (Pragmatique)

**Principe** : Subdiviser chaque batch en micro-batches.

```python
# hubeau_assets.py
BATCH_SIZE = 5000  # Batch principal
MICRO_BATCH = 500  # Micro-batch pour COPY

batch = []
for record in source:
    batch.append(record)

    # Charger par micro-batches
    if len(batch) >= MICRO_BATCH:
        # COPY immédiat du micro-batch
        postgres_bulk_destination.load_micro_batch(
            table_name, batch[:MICRO_BATCH]
        )
        batch = batch[MICRO_BATCH:]  # Garder reste
        gc.collect()  # GC agressif
```

**Avantages** :
- ✅ Réduit peak memory (500 records vs 50k)
- ✅ Compatible avec code existant
- ✅ Facile à implémenter

**Inconvénients** :
- Plus de round-trips PostgreSQL
- Overhead transactions

### Solution 3 : DataFrame Chunking (Pandas Efficient)

**Principe** : Traiter DataFrame par chunks.

```python
def _copy_from_dataframe_chunked(df, table_name, conn, chunk_size=500):
    """COPY par chunks pour économiser RAM"""

    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size]  # Vue (pas copie)

        # Clean chunk
        chunk = self._clean_dataframe_inplace(chunk, table_name, conn)

        # COPY chunk
        output = io.StringIO()
        chunk.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        cursor.copy_expert(copy_sql, output)

        # Libérer immédiatement
        del chunk, output
        gc.collect()
```

**Avantages** :
- ✅ Réduit peak memory significativement
- ✅ Compatible avec cleaning existant
- ✅ Pas de changement API

### Solution 4 : Iterator Pattern (Python Generator)

**Principe** : Remplacer liste par generator.

```python
# AVANT
batch = []  # Liste en RAM
for record in source:
    batch.append(record)

# APRÈS
def batch_generator(source, batch_size):
    """Generator qui yield batches sans stocker tout"""
    batch = []
    for record in source:
        batch.append(record)
        if len(batch) >= batch_size:
            yield batch
            batch = []  # Vider
    if batch:
        yield batch

# Usage
for batch in batch_generator(source, BATCH_SIZE):
    load_batch(batch)  # Batch GC automatique après yield
```

**Avantages** :
- ✅ Pas d'accumulation
- ✅ GC automatique
- ✅ Code propre

### Solution 5 : Éliminer Copies Inutiles

**Principe** : Optimiser _clean_dataframe_inplace pour VRAIMENT modifier en place.

```python
# AVANT (ligne 214 - COPIE!)
original_col = df[col].copy()  # ← COPIE 50 MB

# APRÈS (sans copie)
# Option A: Supprimer la vérification
df[col] = pd.to_numeric(df[col], errors='coerce')

# Option B: Vérifier avant, pas après
if not is_numeric_column(df[col]):
    logger.warning(f"Column {col} has non-numeric data")
```

**Changements** :
1. Supprimer ligne 214 `original_col = df[col].copy()`
2. Supprimer lignes 219-223 (vérification qui utilise original_col)
3. Utiliser vectorisation Pandas pure (pas `.apply()`)

### Solution 6 : Désactiver Caches Inutiles

**Principe** : Cache metadata seulement, pas données.

```python
# Cache OK (metadata)
self._table_columns_cache = {}  # ✅ Petit (noms colonnes)
self._cache_timestamps = {}      # ✅ Petit (timestamps)

# Cache NON (données)
# Ne JAMAIS cacher DataFrames ou records
```

**Actuellement** : Pas de cache de données → OK ✅

## 🎯 Recommandations Immédiates

### Priorité 1 : Réduire Batch Sizes (FAIT)

✅ **ecoulement_observations** : 50k → 5k
✅ **quality_groundwater_stations** : 1k
✅ **hydrobio_stations** : 1k

### Priorité 2 : Implémenter Micro-Batching

Subdiviser chaque batch en micro-batches de 500-1000 records :

```python
# hubeau_assets.py
MICRO_BATCH_SIZE = 500

batch = []
for record in source:
    batch.append(record)

    # Flush par micro-batch
    while len(batch) >= MICRO_BATCH_SIZE:
        micro = batch[:MICRO_BATCH_SIZE]
        postgres_bulk_destination.load_batch(
            table_name, micro, write_disposition, primary_keys
        )
        batch = batch[MICRO_BATCH_SIZE:]
        gc.collect()
```

**Impact** :
- Mémoire peak : 50 MB (500 records) au lieu de 500 MB (50k records)
- **10x réduction mémoire**

### Priorité 3 : Supprimer Copie `original_col`

**Fichier** : `postgres_optimized_v2.py` ligne 214

```python
# SUPPRIMER
original_col = df[col].copy()  # ← Ligne 214

# SUPPRIMER lignes 216-226 (vérification avec original_col)
```

**Impact** :
- Économise 1 copie complète par colonne nettoyée
- **~20-30% réduction mémoire** dans _clean_dataframe

### Priorité 4 : Generator Pattern

Remplacer accumulation liste par generator :

```python
def batch_iterator(source, batch_size):
    """Yield batches au lieu d'accumuler"""
    batch = []
    for record in source:
        batch.append(record)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

# Usage
for batch in batch_iterator(source, BATCH_SIZE):
    load_batch(batch)
```

### Priorité 5 : Monitoring Mémoire

Ajouter logs mémoire pour détecter leaks :

```python
import psutil
import os

def log_memory_usage(context, label):
    """Log mémoire process"""
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024 / 1024
    context.log.info(f"[MEMORY] {label}: {mem_mb:.1f} MB")

# Usage dans assets
log_memory_usage(context, "Avant batch")
load_batch(...)
log_memory_usage(context, "Après batch")
gc.collect()
log_memory_usage(context, "Après GC")
```

## 📈 Gains Estimés

| Optimisation | Réduction RAM | Effort | Priorité |
|--------------|---------------|--------|----------|
| Batch sizes 50k→5k | 90% | ✅ FAIT | P1 |
| Micro-batching | 80% | Moyen | P2 |
| Supprimer original_col | 25% | Faible | P3 |
| Generator pattern | 15% | Faible | P4 |
| Monitoring | 0% (visibilité) | Faible | P5 |
| **TOTAL CUMULÉ** | **95%+** | - | - |

## 🎓 Principes Généraux

1. **Stream, Don't Store** : Traiter données en flux, pas en batch géant
2. **Zero-Copy When Possible** : Éviter copies inutiles (vues Pandas)
3. **Fail Fast on Memory** : Limiter batch size strictement
4. **GC Aggressive** : Forcer garbage collection après chaque batch
5. **Monitor Everything** : Logger mémoire à chaque étape critique

## 🔮 Optimisations Futures

### Long Terme : psycopg3 Native Streaming

```python
# psycopg3 supporte streaming natif
with connection.cursor().copy("COPY table FROM STDIN") as copy:
    for record in source:
        copy.write_row(record.values())
```

**Avantages** :
- Zéro buffer intermédiaire
- Mémoire O(1) constante
- Performance optimale

**Blocage** :
- Requiert migration psycopg2 → psycopg3
- Breaking changes dans l'API
