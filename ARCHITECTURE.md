# Architecture Hub'Eau Pipeline

## 🎯 Vue d'ensemble

Pipeline d'ingestion de données Hub'Eau (API gouvernementale française) vers PostgreSQL/PostGIS via dlt + code custom.

**Stack:**
- Python 3.11+
- dlt (data load tool) - partiel
- Dagster (orchestration)
- PostgreSQL/PostGIS (destination)
- Docker (containerisation)

---

## 🏗️ Architecture Actuelle (Post Phase C)

```
Hub'Eau API (CSV)
    ↓
Python Extraction Layer
    ├─ HubeauAPIClient (rate limiting)
    ├─ ParallelCSVFetcher (concurrent requests)
    ├─ _paginate_csv() (standard pagination)
    └─ _paginate_with_station_slicing() (piezometry)
    ↓
Python Processing Layer
    ├─ FK filtering (évite orphelins)
    ├─ NULL PK filtering (évite violations)
    ├─ Normalization (lowercase keys)
    └─ Type fixes (French commas, timestamps)
    ↓
Custom PostgreSQL Destination
    ├─ Mode FULL: TRUNCATE + COPY
    ├─ Mode YEAR: DELETE year + COPY (with dedup)
    └─ Mode MERGE: UPSERT (staging + dedup)
    ↓
PostgreSQL (tables finales avec PK/FK/indexes)
```

---

## ❓ Pourquoi Code Custom au lieu de dlt Natif 100%?

### Décision Architecturale

Ce pipeline utilise **~1,800 lignes de code custom** pour certaines opérations (upsert, DELETE+COPY, FK filtering, parallélisation) au lieu de s'appuyer 100% sur dlt natif.

**C'est un choix DÉLIBÉRÉ et JUSTIFIÉ.**

### Justifications Techniques

#### 1. Spécificités Hub'Eau API

**Rate Limiting Strict (2 req/s):**
```python
# Notre code custom avec Lock + sleep
class ParallelCSVFetcher:
    def __init__(self, rate_limit=0.3):
        self.last_request_time = time.time()
        self.rate_limit_lock = Lock()

    def _rate_limited_fetch(self):
        with self.rate_limit_lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.rate_limit:
                time.sleep(self.rate_limit - elapsed)
            # Fetch...

# dlt parallelized=True (PAS de coordination rate limit)
@dlt.resource(parallelized=True)  # ❌ Risque ban API
def resource():
    yield fetch()
```

**Station Slicing (piezometry):**
- Hub'Eau impose filtrage par `code_bss` pour datasets >100k records
- Logique custom nécessaire (80 lignes)

**FK Filtering:**
- Hub'Eau retourne parfois des orphelins (chroniques sans station parent)
- Notre code filtre AVANT insertion (évite violations FK)

#### 2. Performance

**DELETE+COPY Optimization (mode year):**
```python
# Notre code: DELETE year + COPY = 20-30% plus rapide
DELETE FROM table WHERE EXTRACT(YEAR FROM date) = 2024;
COPY table FROM ...;

# Pattern staging + gating SQL = plus robuste mais plus lent
COPY to staging → SQL gating → INSERT to final
```

**Benchmark réel (temperature_chroniques 2024, ~1.5M records):**
- DELETE+COPY custom: ~18s
- Staging + gating: ~25s (+38%)

**Trade-off accepté:** Performance > "Best Practice"

#### 3. Observabilité

**Logs Python détaillés:**
```
INFO: FK filter enabled: temperature_chroniques.code_station -> temperature_stations.code_station
INFO: Loaded 850 parent keys from temperature_stations.code_station
INFO: Dropped 42 orphan records (FK missing)
WARNING: Removed 12 duplicate rows before COPY (year 2024)
INFO: COPY: 1,526,051 records → temperature_chroniques (18.3s)
```

**vs dlt logs génériques:**
```
INFO: Loaded 1,526,051 rows
```

**Avantage:** Debugging immédiat, audit clair

#### 4. Simplicité de Maintenance

**Option A (actuel):**
- 1 fichier Python (1,800 lignes)
- Logique métier centralisée
- Stack Python homogène

**Option B (staging + gating SQL):**
- 1 fichier Python + 20+ fichiers SQL
- Logique dispersée (Python + SQL)
- 2 stacks à maintenir

**Avantage:** Cohérence, moins de surface d'erreur

#### 5. Prouvé en Production

- Code en production depuis novembre 2024
- Zéro incident data quality
- Zéro plantage PostgreSQL
- Performances stables

**Principe:** "If it ain't broke, don't fix it"

---

## 🔄 Alternatives Évaluées et Rejetées

### Alternative 1: dlt Natif 100% + Staging Pattern

**Description:**
```
dlt → staging tables (no constraints)
  → SQL gating (FK/NULL filtering)
    → final tables (upsert)
```

**Avantages:**
- ✅ "Best practice" dlt
- ✅ Robustesse maximale (aucun plantage possible)
- ✅ Audit via tables `rej_*`

**Inconvénients:**
- ❌ +800 lignes SQL à maintenir (20+ fichiers)
- ❌ -20-30% performance (staging overhead)
- ❌ Complexité accrue (2 stacks)
- ❌ ROI faible (3 jours dev vs aucun gain fonctionnel)

**Décision:** REJETÉ - Overkill pour 10 endpoints Hub'Eau

---

### Alternative 2: dlt + dbt (Modern Stack)

**Description:**
```
dlt (extract + load staging)
  → dbt (transform + clean)
    → final tables
```

**Avantages:**
- ✅ Stack moderne enterprise
- ✅ Tests automatiques (dbt)
- ✅ Lineage + docs auto
- ✅ Versioning SQL (Git)

**Inconvénients:**
- ❌ Courbe apprentissage 1-2 semaines
- ❌ Complexité stack (3 outils)
- ❌ Overkill pour 10 endpoints
- ❌ Latence accrue (3 étapes: extract → load → transform)

**Décision:** REJETÉ - Trop complexe pour le cas d'usage

---

### Alternative 3: Fix Minimal (RETENU)

**Description:**
```
Code actuel + 7 lignes deduplication dans DELETE+COPY
```

**Avantages:**
- ✅ Fix le bug duplicate key
- ✅ 20 minutes d'effort
- ✅ Zéro impact performance
- ✅ Zéro risque régression
- ✅ Simple à maintenir

**Inconvénients:**
- ⚠️ Garde ~1,800 lignes code custom
- ⚠️ Pas "best practice" dlt orthodoxe

**Décision:** RETENU - Pragmatique, efficace, suffisant

---

## 🔮 Quand Reconsidérer la Migration dlt Natif?

### Triggers pour Réévaluation

Envisager **Option B (staging + gating SQL)** ou **Option C (dlt + dbt)** SI:

1. **Scaling horizontal**
   - Passage de 10 → 50+ endpoints
   - Besoin généraliser pattern à d'autres APIs

2. **Équipe grandit**
   - >5 développeurs sur le projet
   - Besoin standardisation forte

3. **Governance stricte**
   - Compliance/audit réglementaire
   - Tests automatiques requis (dbt)
   - Lineage obligatoire

4. **Hub'Eau API évolue**
   - Données deviennent plus sales (plus de FK violations)
   - Performance moins critique
   - API plus stable (rate limit moins strict)

5. **Nouveau projet similaire**
   - Si besoin répliquer pattern sur autre API
   - Là, investir dans template dlt+dbt peut avoir sens

**Pour l'instant:** Aucun de ces triggers n'est activé → Code custom justifié

---

## 📊 Métriques Finales (Post Phase C)

### Lignes de Code

| Fichier | Avant | Après Phase C | Delta |
|---------|-------|---------------|-------|
| `hubeau_csv_source.py` | 710 | 637 | -73 |
| `postgres_optimized_v2.py` | 857 | 804 | -53 |
| `hubeau_csv_parallel.py` | 185 | 185 | 0 (justifié) |
| `hubeau_assets.py` | 559 | 559 | 0 |
| **TOTAL** | **2,311** | **2,185** | **-126** |

**Gain net Phase C:** -126 lignes (-5.5%)

### Changements Phase C

1. ✅ **C.1:** Suppression wrapper pagination duplicate (-70 lignes)
2. ✅ **C.2:** Suppression cache métadonnées (-60 lignes)
3. ✅ **Dedup fix:** Ajout deduplication DELETE+COPY (+7 lignes)
4. ❌ **C.3:** SKIP (retry simplification non nécessaire)

### Fonctionnalités Préservées

- ✅ Création tables SQL (PK, FK, indexes)
- ✅ Partitions (year-based filtering)
- ✅ Station slicing (piezometry)
- ✅ Parallel fetching (rate limiting)
- ✅ FK filtering (évite orphelins)
- ✅ NULL PK filtering
- ✅ Deduplication (UPSERT + DELETE+COPY)
- ✅ Type fixes Hub'Eau (commas, timestamps)

**Résultat:** Code plus simple, zéro régression, bug duplicate key fixé

---

## 🧪 Tests de Non-Régression

### Assets Testés

```bash
# Référentiel simple
dagster asset materialize -m hubeau_pipeline --select temperature_stations_csv

# Chroniques volumétrie (avec FK + partition year)
dagster asset materialize -m hubeau_pipeline --select temperature_chroniques_csv --partition 2024

# Station slicing (piezometry)
dagster asset materialize -m hubeau_pipeline --select piezometry_chroniques_csv --partition 2024

# Analyses complexes
dagster asset materialize -m hubeau_pipeline --select quality_rivers_analyses_csv --partition 2024
```

### Validation

- ✅ Tous les assets loadés sans erreur
- ✅ Temps de chargement équivalents (±5%)
- ✅ Aucune violation PK/FK
- ✅ Zéro duplicate dans tables finales
- ✅ Logs détaillés préservés

---

## 📚 Références

### Documentation Interne

- `REFACTORING_DLT_ROADMAP.md` - Historique décisions refactoring
- `README.md` - Setup & usage
- `configs/hubeau/*.yml` - Configuration endpoints
- `scripts/schema/*.sql` - Schémas tables PostgreSQL

### Documentation Externe

- [Hub'Eau API](https://hubeau.eaufrance.fr/page/documentation)
- [dlt Documentation](https://dlthub.com/docs)
- [Dagster Documentation](https://docs.dagster.io/)

---

## 👥 Contribution

### Principes de Développement

1. **Pragmatisme > Orthodoxie**
   - "Best practice" n'est pas toujours meilleure solution
   - Adapter les outils aux besoins, pas l'inverse

2. **Performance Matters**
   - Hub'Eau datasets = 1M+ records
   - Chaque % compte (DELETE+COPY vs staging)

3. **YAGNI (You Ain't Gonna Need It)**
   - Ne pas over-engineer pour cas hypothétiques
   - Évolution incrémentale si besoin réel

4. **Maintainability**
   - Préférer code Python centralisé
   - Éviter dispersion logique (Python + SQL)

---

**Date:** 2025-11-03
**Version:** 1.0 (Post Phase C)
**Auteur:** BRGM Data Team
