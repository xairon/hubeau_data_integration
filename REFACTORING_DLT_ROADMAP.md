# ROADMAP REFACTORING DLT - Hub'Eau Pipeline

**Projet:** Hub'Eau Data Ingestion Pipeline
**Objectif:** Réduire la complexité du code en utilisant mieux dlt
**Date création:** 2025-11-03
**Status:** 🟡 EN COURS - Phase de planification

---

## 📊 ÉTAT ACTUEL (BASELINE)

### Métriques du Projet

| Fichier | Lignes | Rôle | % Réinventé |
|---------|--------|------|-------------|
| `hubeau_csv_source.py` | 710 | Pagination + HTTP client | 63% |
| `hubeau_csv_parallel.py` | 185 | Parallélisation custom | 86% |
| `postgres_optimized_v2.py` | 857 | COPY + Upsert custom | 76% |
| `hubeau_assets.py` | 559 | Orchestration Dagster | 45% |
| **TOTAL** | **2,311 lignes** | Infrastructure data | **~65%** |

### Code Réinventé vs Justifié

- **~1,500 lignes (65%)** : Réinventions de fonctionnalités dlt natives
- **~800 lignes (35%)** : Code justifié (orchestration + spécificités Hub'Eau)

### Fonctionnalités Réinventées

| Fonctionnalité | Ton Code | DLT Natif | Gain Potentiel |
|----------------|----------|-----------|----------------|
| Pagination API | 89 lignes | `@dlt.resource` + yield | -89 lignes |
| Parallélisation | 118 lignes | Arrow/Parquet loader | -118 lignes |
| COPY PostgreSQL | 181 lignes | `loader_file_format="csv"` | -181 lignes |
| Upsert/Merge | 187 lignes | `write_disposition="merge"` | -187 lignes |
| Pool connexions | 13 lignes | dlt auto | -13 lignes |
| Cache schéma | 79 lignes | dlt auto | -79 lignes |
| Type casting | 124 lignes | dlt type hints | -80 lignes |
| Retry/backoff | 40 lignes | dlt retry policy | -40 lignes |
| **TOTAL** | **831 lignes** | | **-787 lignes** |

### Code Justifié à Conserver

✅ **Station slicing** (80 lignes) - Hub'Eau impose `code_bss` pour piezometry
✅ **FK filtering** (50 lignes) - Évite orphelins dans chroniques
✅ **French comma fix** (14 lignes) - "994,4" → "994.4"
✅ **Timestamp ms fix** (18 lignes) - Epoch ms → datetime
✅ **Orchestration Dagster** (150 lignes) - Config projet spécifique

**Total justifié:** ~312 lignes (13%)

---

## 🎯 STRATÉGIE DE MIGRATION (3 PHASES)

### Phase C → Phase B → Phase A

```
ACTUEL (2,311 lignes)
    ↓
PHASE C: Optimisation Minimale (-7%, 4-6h)
    ↓ [Test & Validation]
PHASE B: Migration Hybride (-52%, 1-2 jours)
    ↓ [Test & Validation]
PHASE A: Migration Complète (-74%, optionnel)
    ↓
CIBLE (500-600 lignes)
```

**Principe:** Migrer progressivement avec validation à chaque étape.

---

## 📋 PHASE C: OPTIMISATION MINIMALE

**Status:** 🔵 PRÊT À DÉMARRER
**Durée estimée:** 4-6 heures
**Risque:** Très faible (pas de changement architectural)
**Gain:** -160 lignes (-7%)

### Objectif

Nettoyer le code sans toucher à l'architecture actuelle. Supprimer les doublons évidents et simplifier les fonctions redondantes.

### Changements Détaillés

#### C.1 - Fusionner pagination dupliquée (hubeau_csv_source.py)

**Fichier:** `src/hubeau_pipeline/sources/hubeau_csv_source.py`

**Problème:** Duplication entre `_paginate_csv()` (ligne 310-399) et `get_raw_data_iterator()` (ligne 640-710)

**Action:**
- Supprimer `get_raw_data_iterator()` (70 lignes)
- Utiliser directement `_paginate_csv()` dans les assets
- Modifier `hubeau_assets.py` ligne 325 pour appeler `_paginate_csv()` au lieu du wrapper

**Gain:** -70 lignes

**Fichiers modifiés:**
- `hubeau_csv_source.py` (suppression fonction)
- `hubeau_assets.py` (changement import ligne 325)

**Test de validation:**
```bash
# Lancer un asset simple pour vérifier pagination
dagster asset materialize -m hubeau_pipeline -s temperature_stations_csv
```

---

#### C.2 - Supprimer cache métadonnées custom (postgres_optimized_v2.py)

**Fichier:** `src/hubeau_pipeline/destinations/postgres_optimized_v2.py`

**Problème:** Cache custom des colonnes tables (lignes 95-174) alors que dlt gère le schéma automatiquement

**Action:**
- Garder `_get_target_columns()` mais supprimer le cache (lignes 100-108, 127-129)
- Supprimer `_get_table_column_types()` cache (lignes 141-148, 166-168)
- Supprimer `_table_columns_cache`, `_cache_ttl`, `_cache_timestamps` (lignes 66-68)
- Supprimer `clear_cache()` (lignes 817-821)

**Gain:** -79 lignes

**Fichiers modifiés:**
- `postgres_optimized_v2.py` (suppression cache)

**Test de validation:**
```bash
# Vérifier que les requêtes information_schema ne ralentissent pas
# Lancer un chargement avec monitoring temps
dagster asset materialize -m hubeau_pipeline -s temperature_chroniques_csv --partition 2024
```

---

#### C.3 - Simplifier retry/rate limiting (hubeau_csv_source.py)

**Fichier:** `src/hubeau_pipeline/sources/hubeau_csv_source.py`

**Problème:** Retry manuel dans `fetch_csv_page()` (lignes 239-301) redondant avec retry de `requests.Session`

**Action:**
- Garder retry strategy dans `HubeauAPIClient.__init__()` (lignes 62-67)
- Simplifier `fetch_csv_page()` : supprimer boucle while retry (lignes 239-242, 289-301)
- Laisser `requests` gérer les retries automatiquement

**Gain:** -30 lignes

**Fichiers modifiés:**
- `hubeau_csv_source.py` (simplification retry)

**Test de validation:**
```bash
# Tester avec un endpoint instable (simule timeout)
# Observer les logs de retry automatique
dagster asset materialize -m hubeau_pipeline -s hydrobio_indices_csv --partition 2024
```

---

### Checklist Phase C

- [ ] **C.1** Fusionner pagination dupliquée (-70 lignes)
  - [ ] Modifier imports dans `hubeau_assets.py`
  - [ ] Supprimer `get_raw_data_iterator()`
  - [ ] Tester asset `temperature_stations_csv`

- [ ] **C.2** Supprimer cache métadonnées (-79 lignes)
  - [ ] Retirer variables cache du `__init__`
  - [ ] Simplifier `_get_target_columns()`
  - [ ] Simplifier `_get_table_column_types()`
  - [ ] Supprimer `clear_cache()`
  - [ ] Tester asset `temperature_chroniques_csv` partition 2024

- [ ] **C.3** Simplifier retry/rate limiting (-30 lignes)
  - [ ] Supprimer boucle while dans `fetch_csv_page()`
  - [ ] Tester asset `hydrobio_indices_csv` partition 2024

- [ ] **Validation finale Phase C**
  - [ ] Lancer 3 assets de types différents (stations + chroniques + analyses)
  - [ ] Vérifier logs (pas d'erreurs, temps similaires)
  - [ ] Compter lignes de code avant/après
  - [ ] Commit git: `git commit -m "refactor(phase-c): simplify code -160 lines"`

**Résultat attendu:** 2,311 → 2,151 lignes (-7%)

---

## 📋 PHASE B: MIGRATION HYBRIDE

**Status:** ⏸️ EN ATTENTE (après Phase C validée)
**Durée estimée:** 1-2 jours
**Risque:** Moyen (changement de stratégie pagination/upsert)
**Gain:** -1,200-1,400 lignes (-52-60%)

### Objectif

Utiliser dlt pour pagination et upsert, garder COPY custom uniquement si critique pour perfs.

### Changements Détaillés

#### B.1 - Supprimer parallélisation custom → dlt Arrow

**Fichier:** `src/hubeau_pipeline/sources/hubeau_csv_parallel.py`

**Problème:** Réinvention complète de la parallélisation (185 lignes) alors que dlt + Arrow le fait nativement

**Action:**
1. Supprimer `hubeau_csv_parallel.py` entièrement
2. Dans `hubeau_csv_source.py`, modifier `@dlt.resource` pour yielder Arrow tables au lieu de dicts:

```python
# AVANT (ligne 591):
def csv_resource() -> Iterator[List[Dict]]:
    # ...
    yield batch  # List[Dict]

# APRÈS:
def csv_resource() -> Iterator[pa.Table]:
    import pyarrow as pa
    # ...
    # Convertir DataFrame → Arrow Table
    arrow_table = pa.Table.from_pandas(df)
    yield arrow_table
```

3. Dans `hubeau_assets.py`, utiliser `loader_file_format="parquet"` pour parallélisation auto:

```python
# Ligne 405-409 (REMPLACER):
pipeline.run(
    csv_resource,
    loader_file_format="parquet",  # ← dlt parallélise automatiquement
    write_disposition="merge"
)
```

**Gain:** -185 lignes (fichier entier supprimé)

**Fichiers modifiés:**
- `hubeau_csv_parallel.py` (suppression fichier)
- `hubeau_csv_source.py` (yield Arrow tables)
- `hubeau_assets.py` (loader_file_format)
- `pyproject.toml` (ajouter pyarrow si absent)

**Test de validation:**
```bash
# Tester avec un gros endpoint (1M+ records)
dagster asset materialize -m hubeau_pipeline -s temperature_chroniques_csv --partition 2024

# Comparer durée AVANT/APRÈS (doit être similaire ou plus rapide)
```

---

#### B.2 - Utiliser dlt merge natif au lieu de upsert custom

**Fichier:** `src/hubeau_pipeline/destinations/postgres_optimized_v2.py`

**Problème:** `_upsert_dataframe()` (187 lignes) réinvente `write_disposition="merge"` de dlt

**Action:**
1. Tester `write_disposition="merge"` dlt natif sur 1 endpoint pilote
2. Comparer performances (temps, mémoire) vs upsert custom
3. Si performances OK (±10%), supprimer `_upsert_dataframe()`
4. Dans `hubeau_assets.py`, utiliser directement `pipeline.run()` au lieu de `load_batch()`

```python
# AVANT (ligne 468-475):
postgres_bulk_destination.load_batch(
    table_name=table_name,
    data=page_records,
    write_disposition=batch_write_disposition,
    primary_keys=primary_keys,
    # ...
)

# APRÈS:
@dlt.resource(
    name=table_name,
    write_disposition="merge",
    primary_key=primary_keys
)
def data_resource():
    yield arrow_table  # ou pa.Table.from_pylist(page_records)

pipeline.run(data_resource)
```

**Gain:** -187 lignes (si perf OK)

**Fichiers modifiés:**
- `postgres_optimized_v2.py` (suppression `_upsert_dataframe`)
- `hubeau_assets.py` (utiliser `pipeline.run()` au lieu de `load_batch()`)

**Test de validation:**
```bash
# Endpoint pilote: temperature_chroniques (volumétrie moyenne)
# 1. Mesurer temps AVANT (avec _upsert_dataframe custom)
time dagster asset materialize -m hubeau_pipeline -s temperature_chroniques_csv --partition 2024

# 2. Migrer vers dlt merge natif
# 3. Mesurer temps APRÈS (avec write_disposition="merge" dlt)
time dagster asset materialize -m hubeau_pipeline -s temperature_chroniques_csv --partition 2024

# 4. Comparer (écart max acceptable: ±10%)
# 5. Vérifier intégrité données (count, duplicates, FK)
```

**Critères de succès:**
- Temps ±10%
- Zéro duplicates (vérifier `SELECT code_station, date, COUNT(*) ... HAVING COUNT(*) > 1`)
- FK respectées (vérifier `SELECT COUNT(*) FROM chroniques c LEFT JOIN stations s ...`)

**Si échec:** Garder `_upsert_dataframe()` custom, passer à B.3

---

#### B.3 - Remplacer pagination custom par yield dlt simple

**Fichier:** `src/hubeau_pipeline/sources/hubeau_csv_source.py`

**Problème:** `_paginate_csv()` (89 lignes) réinvente la pagination alors que dlt le fait avec yield

**Action:**
1. Simplifier `csv_resource()` pour utiliser yield direct au lieu de `_paginate_csv()`:

```python
# AVANT (ligne 622-630):
for batch in _paginate_csv(client, endpoint, params, resource_name):
    total_records_yielded['count'] += len(batch)
    yield batch

# APRÈS (simplifié):
page = 1
while True:
    df = fetch_csv_page(client, endpoint, page, params)
    if df.empty:
        break
    yield pa.Table.from_pandas(df)  # Yield Arrow directement
    page += 1
```

2. Supprimer `_paginate_csv()` (ligne 310-399, 89 lignes)
3. Garder uniquement `_paginate_with_station_slicing()` (spécifique Hub'Eau)

**Gain:** -89 lignes

**Fichiers modifiés:**
- `hubeau_csv_source.py` (simplification pagination)

**Test de validation:**
```bash
# Tester pagination sur endpoint sans station slicing
dagster asset materialize -m hubeau_pipeline -s quality_rivers_analyses_csv --partition 2024

# Tester station slicing (doit encore marcher)
dagster asset materialize -m hubeau_pipeline -s piezometry_chroniques_csv --partition 2024
```

---

#### B.4 - Optionnel: Tester dlt COPY natif vs custom

**Fichier:** `src/hubeau_pipeline/destinations/postgres_optimized_v2.py`

**Problème:** `_copy_from_dataframe()` (181 lignes) réinvente COPY alors que dlt le fait avec `loader_file_format="csv"`

**Action (OPTIONNEL, si temps disponible):**
1. Tester `loader_file_format="csv"` dlt natif
2. Comparer performances vs `_copy_from_dataframe()` custom
3. Si dlt COPY aussi rapide (±5%), supprimer custom

**Gain potentiel:** -181 lignes

**Test de validation:**
```bash
# 1. Benchmark COPY custom actuel
time dagster asset materialize -m hubeau_pipeline -s temperature_stations_csv

# 2. Migrer vers dlt COPY natif
# 3. Benchmark dlt COPY
time dagster asset materialize -m hubeau_pipeline -s temperature_stations_csv

# Critère: doit être dans ±5% (COPY est critique pour perf)
```

**Si échec:** Garder `_copy_from_dataframe()` custom

---

### Checklist Phase B

- [ ] **B.1** Supprimer parallélisation custom (-185 lignes)
  - [ ] Ajouter pyarrow au `pyproject.toml`
  - [ ] Modifier `csv_resource()` pour yield Arrow tables
  - [ ] Supprimer `hubeau_csv_parallel.py`
  - [ ] Modifier `hubeau_assets.py` pour `loader_file_format="parquet"`
  - [ ] Tester asset `temperature_chroniques_csv` partition 2024 (comparer durée)

- [ ] **B.2** Utiliser dlt merge natif (-187 lignes)
  - [ ] Test pilote sur `temperature_chroniques` partition 2024
  - [ ] Mesurer temps AVANT/APRÈS
  - [ ] Vérifier intégrité (count, duplicates, FK)
  - [ ] Si OK: supprimer `_upsert_dataframe()` et migrer tous les assets
  - [ ] Tester 3-5 endpoints différents

- [ ] **B.3** Simplifier pagination (-89 lignes)
  - [ ] Remplacer `_paginate_csv()` par yield direct
  - [ ] Garder `_paginate_with_station_slicing()` (spécifique Hub'Eau)
  - [ ] Tester asset `quality_rivers_analyses_csv` partition 2024
  - [ ] Tester asset `piezometry_chroniques_csv` partition 2024 (station slicing)

- [ ] **B.4** OPTIONNEL: Tester dlt COPY natif (-181 lignes)
  - [ ] Benchmark COPY custom actuel
  - [ ] Tester `loader_file_format="csv"` dlt
  - [ ] Comparer performances (±5%)
  - [ ] Décision: garder custom ou migrer dlt

- [ ] **Validation finale Phase B**
  - [ ] Test complet sur 5-7 endpoints représentatifs:
    - Stations (référentiel)
    - Chroniques (volumétrie haute)
    - Analyses (volumétrie moyenne)
    - Piezometry (avec station slicing)
  - [ ] Vérifier métriques:
    - Temps de chargement (±10% acceptable)
    - Mémoire utilisée
    - Intégrité données (count, duplicates, FK)
  - [ ] Compter lignes de code avant/après
  - [ ] Commit git: `git commit -m "refactor(phase-b): migrate to dlt native -1200 lines"`

**Résultat attendu:** 2,151 → 900-1,100 lignes (-52-60%)

---

## 📋 PHASE A: MIGRATION COMPLÈTE

**Status:** ⏸️ EN ATTENTE (après Phase B validée)
**Durée estimée:** 2-3 jours
**Risque:** Élevé (refactoring profond)
**Gain:** -1,700 lignes (-74%)
**OPTIONNEL:** Seulement si Phase B montre que dlt natif = perfs équivalentes

### Objectif

Utiliser dlt à 100% (sauf orchestration Dagster), ne garder que les spécificités Hub'Eau.

### Changements Détaillés

#### A.1 - Supprimer destination custom PostgreSQL entièrement

**Fichier:** `src/hubeau_pipeline/destinations/postgres_optimized_v2.py`

**Action:**
- Supprimer fichier entièrement (857 lignes)
- Utiliser `dlt.destinations.postgres()` directement
- Migrer type fixes spécifiques Hub'Eau vers preprocessing:

```python
# Dans hubeau_csv_source.py, AVANT yield:
def csv_resource():
    # ...
    df = fetch_csv_page(...)

    # Type fixes Hub'Eau (garder)
    df = fix_french_commas(df)  # "994,4" → "994.4"
    df = fix_timestamp_ms(df)   # epoch ms → datetime

    yield pa.Table.from_pandas(df)
```

**Gain:** -857 lignes (fichier entier)

**Fichiers modifiés:**
- `postgres_optimized_v2.py` (suppression fichier)
- `hubeau_csv_source.py` (ajout preprocessing)
- `hubeau_assets.py` (utiliser `dlt.destinations.postgres()`)

---

#### A.2 - Simplifier HubeauAPIClient

**Fichier:** `src/hubeau_pipeline/sources/hubeau_csv_source.py`

**Action:**
- Garder rate limiting (spécifique Hub'Eau)
- Supprimer retry custom (dlt le gère)
- Réduire `HubeauAPIClient` à ~20 lignes

**Gain:** -30 lignes

---

#### A.3 - Nettoyer hubeau_assets.py

**Fichier:** `src/hubeau_pipeline/assets/hubeau_assets.py`

**Action:**
- Simplifier boucle d'ingestion (lignes 412-481)
- Utiliser `pipeline.run(resource)` directement
- Supprimer FK filtering custom (faire en SQL AFTER constraint)

**Gain:** -150 lignes

---

### Checklist Phase A

- [ ] **A.1** Supprimer destination custom (-857 lignes)
  - [ ] Migrer type fixes vers preprocessing
  - [ ] Utiliser `dlt.destinations.postgres()`
  - [ ] Tester endpoint pilote

- [ ] **A.2** Simplifier HubeauAPIClient (-30 lignes)
  - [ ] Retirer retry custom
  - [ ] Garder rate limiting

- [ ] **A.3** Nettoyer hubeau_assets.py (-150 lignes)
  - [ ] Simplifier boucle ingestion
  - [ ] Utiliser `pipeline.run()` directement

- [ ] **Validation finale Phase A**
  - [ ] Test complet sur TOUS les endpoints
  - [ ] Vérifier perfs (temps, mémoire)
  - [ ] Vérifier intégrité données
  - [ ] Commit git: `git commit -m "refactor(phase-a): full dlt migration -1700 lines"`

**Résultat attendu:** 2,311 → 500-600 lignes (-74%)

---

## 🧪 STRATÉGIE DE TEST PAR PHASE

### Tests Phase C (4-6h)

**Objectif:** Vérifier que simplifications ne cassent rien

```bash
# 1. Asset stations (référentiel simple)
dagster asset materialize -m hubeau_pipeline -s temperature_stations_csv

# 2. Asset chroniques (volumétrie)
dagster asset materialize -m hubeau_pipeline -s temperature_chroniques_csv --partition 2024

# 3. Asset analyses (complexe)
dagster asset materialize -m hubeau_pipeline -s quality_rivers_analyses_csv --partition 2024

# Validation: temps ±5%, zéro erreur
```

---

### Tests Phase B (1-2 jours)

**Objectif:** Valider dlt natif = performances équivalentes

#### Test 1: Parallélisation Arrow (B.1)

```bash
# Endpoint gros volume
time dagster asset materialize -m hubeau_pipeline -s temperature_chroniques_csv --partition 2024

# Comparer AVANT (parallel custom) vs APRÈS (Arrow dlt)
# Critère: ±10% acceptable
```

#### Test 2: Merge dlt natif (B.2)

```bash
# 1. Charger partition 2024
dagster asset materialize -m hubeau_pipeline -s temperature_chroniques_csv --partition 2024

# 2. Recharger même partition (test upsert)
dagster asset materialize -m hubeau_pipeline -s temperature_chroniques_csv --partition 2024

# 3. Vérifier intégrité:
psql -h localhost -U postgres -d postgres -c "
  SELECT COUNT(*) AS total,
         COUNT(DISTINCT code_station) AS stations,
         COUNT(*) - COUNT(DISTINCT code_station || date_mesure) AS duplicates
  FROM hubeau.temperature_chroniques
  WHERE EXTRACT(YEAR FROM date_mesure) = 2024;
"

# Critères:
# - duplicates = 0
# - total cohérent avec API count
# - temps ±10%
```

#### Test 3: Pagination simplifiée (B.3)

```bash
# Sans station slicing
dagster asset materialize -m hubeau_pipeline -s quality_rivers_analyses_csv --partition 2024

# Avec station slicing (doit encore marcher)
dagster asset materialize -m hubeau_pipeline -s piezometry_chroniques_csv --partition 2024
```

---

### Tests Phase A (2-3 jours)

**Objectif:** Validation complète système

```bash
# Test TOUS les endpoints en parallèle
dagster asset materialize \
  -m hubeau_pipeline \
  -s temperature_stations_csv \
  -s temperature_chroniques_csv --partition 2024 \
  -s piezometry_stations_csv \
  -s piezometry_chroniques_csv --partition 2024 \
  -s quality_rivers_stations_csv \
  -s quality_rivers_analyses_csv --partition 2024 \
  -s hydrometry_sites_csv \
  -s hydrometry_stations_csv \
  -s hydrometry_obs_elab_csv --partition 2024

# Vérifier logs: zéro erreur, temps raisonnables
```

---

## 📊 MÉTRIQUES DE SUCCÈS

### Critères Quantitatifs

| Métrique | Avant | Phase C | Phase B | Phase A |
|----------|-------|---------|---------|---------|
| Lignes de code | 2,311 | 2,151 | 900-1,100 | 500-600 |
| Fichiers modifiés | N/A | 2 | 4 | 5 |
| Temps ingestion | Baseline | ±5% | ±10% | ±10% |
| Mémoire utilisée | Baseline | ±5% | ±15% | ±15% |

### Critères Qualitatifs

✅ **Maintenabilité:** Code plus simple, moins de custom
✅ **Documentation:** Roadmap + commentaires clairs
✅ **Robustesse:** Tests passent, zéro régression
✅ **Performances:** Temps chargement acceptable

---

## 🚨 RISQUES & MITIGATION

### Risques Phase C (Faible)

| Risque | Impact | Probabilité | Mitigation |
|--------|--------|-------------|------------|
| Suppression cache ralentit queries | Moyen | Faible | Benchmark avant/après, rollback si >10% |
| Retry simplifié cause timeout | Moyen | Faible | Garder retry si échec tests |

### Risques Phase B (Moyen)

| Risque | Impact | Probabilité | Mitigation |
|--------|--------|-------------|------------|
| dlt merge plus lent que custom | Élevé | Moyen | Test pilote 1 endpoint, rollback si >10% |
| Arrow loader incompatible Hub'Eau | Élevé | Faible | Test endpoint simple d'abord |
| Pagination simplifiée casse station slicing | Élevé | Faible | Tests dédiés piezometry |

### Risques Phase A (Élevé)

| Risque | Impact | Probabilité | Mitigation |
|--------|--------|-------------|------------|
| dlt destination perfs insuffisantes | Critique | Moyen | Phase A optionnelle, garder custom si besoin |
| Type casting dlt incompatible | Élevé | Moyen | Preprocessing Hub'Eau avant yield |
| FK filtering manquant | Moyen | Faible | Migrer en SQL constraints |

---

## 📝 SUIVI & GESTION DES TOKENS

### Conversations Estimées

- **Phase C:** 1 conversation (~30k tokens)
- **Phase B:** 2-3 conversations (~60-80k tokens)
- **Phase A:** 2-3 conversations (~60-80k tokens)

**Total:** 5-7 conversations, ~150-190k tokens

### Template Nouvelle Conversation

```markdown
# CONTEXTE - Refactoring DLT Hub'Eau

**Roadmap:** Voir `REFACTORING_DLT_ROADMAP.md`

**Phase actuelle:** [C/B/A]

**Status dernier checkpoint:**
- ✅ Changement X complété
- ✅ Test Y validé
- 🟡 Changement Z en cours
- ⏸️ Changement W en attente

**Prochaine étape:** [décrire la tâche précise]

**Fichiers concernés:**
- `chemin/fichier1.py` (lignes X-Y)
- `chemin/fichier2.py` (lignes Z-W)

**Question/Action demandée:** [décrire précisément]
```

---

## 🎯 PROCHAINE ACTION

**DÉMARRER PHASE C - Étape C.1**

```bash
# 1. Créer branche git
git checkout -b refactor/phase-c-minimal-optimization

# 2. Modifier imports dans hubeau_assets.py ligne 325
# AVANT:
from hubeau_pipeline.sources.hubeau_csv_source import get_raw_data_iterator

# APRÈS:
from hubeau_pipeline.sources.hubeau_csv_source import _paginate_csv

# 3. Supprimer fonction get_raw_data_iterator() dans hubeau_csv_source.py (lignes 640-710)

# 4. Tester
dagster asset materialize -m hubeau_pipeline -s temperature_stations_csv

# 5. Si OK, commit
git add .
git commit -m "refactor(c.1): merge duplicate pagination functions -70 lines"
```

**Prêt à commencer?** Dis-moi GO et je démarre C.1 !

---

## 📚 RÉFÉRENCES

### Documentation dlt

- **Arrow/Parquet loader:** https://dlthub.com/docs/dlt-ecosystem/file-formats/parquet
- **Write disposition merge:** https://dlthub.com/docs/general-usage/incremental-loading#merge-write-disposition
- **PostgreSQL destination:** https://dlthub.com/docs/dlt-ecosystem/destinations/postgres
- **Type hints:** https://dlthub.com/docs/general-usage/schema#data-types

### Fichiers Projet

- **Config endpoints:** `configs/hubeau/*.yml`
- **Schémas SQL:** `scripts/schema/*.sql`
- **Type mappings:** `src/hubeau_pipeline/schema/hubeau_type_mappings.py`

---

**Date dernière mise à jour:** 2025-11-03
**Auteur:** Assistant Claude
**Reviewer:** User (BRGM)
