# Optimisation TimescaleDB : suppression hypertables Silver + incremental_predicates Gold

**Date** : 2026-03-02
**Statut** : En attente d'approbation
**Impact** : 11 modeles dbt modifies, 0 changement de logique metier

---

## 1. Probleme

### Symptome
Le job schedule hydro tourne 6h+ au lieu de ~5 min. La requete bloquante :
```sql
DELETE FROM silver.stg_hydrometry_obs_elab
USING stg_hydrometry_obs_elab__dbt_tmp
WHERE target.code_site = tmp.code_site
  AND target.date_obs_elab = tmp.date_obs_elab
  AND target.grandeur_hydro_elab = tmp.grandeur_hydro_elab;
```

### Cause racine
La strategie `delete+insert` de dbt genere un DELETE **sans filtre temporel direct**. Sur une hypertable TimescaleDB avec 61 chunks dont 60 comprimes, PostgreSQL doit **decompresser et scanner chaque chunk** pour verifier les correspondances de cle. Le query planner ne peut pas deduire de la JOIN condition que seuls les 7 derniers jours sont concernes.

**Cle technique** : TimescaleDB peut exclure des chunks avec un `WHERE date >= X` direct (chunk exclusion), mais PAS avec un `WHERE date = join_table.date` (le planner ne connait pas les valeurs dans la table jointe).

### Modeles affectes (11 au total)

| Couche | Modele | Rows estimees | Chunks | Risque |
|--------|--------|---------------|--------|--------|
| Silver | `stg_hydrometry_obs_elab` | ~40M | 61 (60 comprimes) | **CRITIQUE** |
| Silver | `stg_piezo_chroniques` | ~50M | ~60 (comprimes) | **CRITIQUE** |
| Silver | `stg_era5_timeseries` | ~100M | ~300 (mensuels, comprimes) | **CRITIQUE** |
| Gold | `int_daily_measurements` | ~50M | ~60 | Modere (compress@365d) |
| Gold | `int_hydro_daily_measurements` | ~40M | ~25 | Modere |
| Gold | `int_era5_for_stations` | ~10M | ~20 | Faible |
| Gold | `int_era5_for_hydro_stations` | ~5M | ~10 | Faible |
| Gold | `hubeau_daily_chroniques` | ~50M | ~60 | Modere |
| Gold | `hydro_daily_chroniques` | ~40M | ~25 | Modere |
| Gold | `fct_monthly_chroniques` | ~1M | ~5 | Faible |
| Gold | `fct_monthly_hydro` | ~0.5M | ~3 | Faible |

---

## 2. Solution : approche hybride pragmatique

### Principe
- **Silver** : retirer les hypertables. Tables PostgreSQL classiques avec index. `delete+insert` rapide sur table indexee.
- **Gold** : garder les hypertables + compression. Ajouter `incremental_predicates` (dbt 1.6+) pour injecter un filtre temporel dans le DELETE, activant le chunk pruning de TimescaleDB.

### Pourquoi cette approche

| Critere | Silver sans hypertable | Gold avec predicates |
|---------|----------------------|---------------------|
| Performance DELETE | Index btree O(log n) | Chunk exclusion (1-2 chunks) |
| Compression | Non (tables transitoires) | Oui (conservation longue) |
| Complexite code | -2 lignes par modele | +3 lignes par modele |
| Migration | `--full-refresh` requis | Aucune migration |
| Risque | Faible (plain SQL) | Faible (feature dbt native) |

---

## 3. Plan d'implementation detaille

### Phase 1 : Silver - Retirer les hypertables (3 modeles)

#### 1.1 `stg_hydrometry_obs_elab` (PRIORITE : urgente)

**Fichier** : `src/dbt_hubeau/models/staging/stg_hydrometry_obs_elab.sql`

**Avant** :
```python
post_hook=[
    "{{ add_primary_key(['code_site', 'date_obs_elab', 'grandeur_hydro_elab']) }}",
    "{{ convert_to_hypertable('date_obs_elab', '1 year') }}",
    "{{ add_foreign_key(['code_site'], 'stg_hydrometry_sites', ['code_site']) }}",
    "{{ enable_compression(segment_by=['code_site'], order_by='date_obs_elab DESC', compress_after='90 days') }}"
]
```

**Apres** :
```python
post_hook=[
    "{{ add_primary_key(['code_site', 'date_obs_elab', 'grandeur_hydro_elab']) }}",
    "{{ add_foreign_key(['code_site'], 'stg_hydrometry_sites', ['code_site']) }}"
]
```

**Migration** : `docker exec brgm-dlt-worker dbt run --full-refresh --select stg_hydrometry_obs_elab`

#### 1.2 `stg_piezo_chroniques`

**Fichier** : `src/dbt_hubeau/models/staging/stg_piezo_chroniques.sql`

Meme modification : retirer `convert_to_hypertable` et `enable_compression` du `post_hook`.

**Migration** : `docker exec brgm-dlt-worker dbt run --full-refresh --select stg_piezo_chroniques`

#### 1.3 `stg_era5_timeseries`

**Fichier** : `src/dbt_hubeau/models/staging/stg_era5_timeseries.sql`

Meme modification.

**Migration** : `docker exec brgm-dlt-worker dbt run --full-refresh --select stg_era5_timeseries`

---

### Phase 2 : Gold - Ajouter incremental_predicates (8 modeles)

La feature `incremental_predicates` (dbt-core 1.6+, supporte par dbt-postgres 1.7.0) ajoute une clause WHERE au DELETE genere par `delete+insert`. Cela permet a TimescaleDB d'exclure les chunks comprimes du scan.

**Syntaxe** : le predicat utilise `DBT_INTERNAL_DEST` comme alias de la table cible.

**Strategie de buffer** : le predicat utilise un buffer 4x plus large que le lookback du SELECT. Ceci garantit que toutes les lignes a supprimer sont couvertes, tout en excluant les vieux chunks comprimes.

| Modele | Lookback SELECT | Buffer predicat | Seuil compression |
|--------|----------------|-----------------|-------------------|
| `int_daily_measurements` | 7 jours | 30 jours | 365 jours |
| `int_hydro_daily_measurements` | 7 jours | 30 jours | 365 jours |
| `int_era5_for_stations` | append (>) | 30 jours | 365 jours |
| `int_era5_for_hydro_stations` | append (>) | 30 jours | 365 jours |
| `hubeau_daily_chroniques` | 7 jours | 30 jours | 365 jours |
| `hydro_daily_chroniques` | 7 jours | 30 jours | 365 jours |
| `fct_monthly_chroniques` | 25 mois | 30 mois | 730 jours |
| `fct_monthly_hydro` | 25 mois | 30 mois | 730 jours |

#### 2.1 `int_daily_measurements`

**Fichier** : `src/dbt_hubeau/models/intermediate/int_daily_measurements.sql`

Ajouter dans le `config()` :
```python
config(
    materialized = 'incremental',
    unique_key = ['code_bss', 'date_mesure'],
    incremental_strategy = 'delete+insert',
    incremental_predicates = [
        "DBT_INTERNAL_DEST.date_mesure >= CURRENT_DATE - INTERVAL '30 days'"
    ],
    ...
)
```

#### 2.2 `int_hydro_daily_measurements`

```python
incremental_predicates = [
    "DBT_INTERNAL_DEST.date_obs_elab >= CURRENT_DATE - INTERVAL '30 days'"
],
```

#### 2.3 `int_era5_for_stations`

```python
incremental_predicates = [
    "DBT_INTERNAL_DEST.era5_date >= CURRENT_DATE - INTERVAL '30 days'"
],
```

#### 2.4 `int_era5_for_hydro_stations`

```python
incremental_predicates = [
    "DBT_INTERNAL_DEST.era5_date >= CURRENT_DATE - INTERVAL '30 days'"
],
```

#### 2.5 `hubeau_daily_chroniques`

```python
incremental_predicates = [
    "DBT_INTERNAL_DEST.date >= CURRENT_DATE - INTERVAL '30 days'"
],
```

#### 2.6 `hydro_daily_chroniques`

```python
incremental_predicates = [
    "DBT_INTERNAL_DEST.date >= CURRENT_DATE - INTERVAL '30 days'"
],
```

#### 2.7 `fct_monthly_chroniques`

```python
incremental_predicates = [
    "DBT_INTERNAL_DEST.mois >= CURRENT_DATE - INTERVAL '30 months'"
],
```

#### 2.8 `fct_monthly_hydro`

```python
incremental_predicates = [
    "DBT_INTERNAL_DEST.mois >= CURRENT_DATE - INTERVAL '30 months'"
],
```

**Aucune migration requise** : le prochain run incremental utilisera automatiquement le predicat.

---

### Phase 3 : Validation

#### 3.1 Compilation
```bash
docker exec brgm-dlt-worker dbt compile
```
Verifier : 0 erreurs, meme nombre de modeles (31).

#### 3.2 Full-refresh Silver (fenetre de maintenance)

Ordre recommande (plus petit au plus gros) :
```bash
# 1. ERA5 timeseries (le plus long, ~100M rows)
docker exec brgm-dlt-worker dbt run --full-refresh --select stg_era5_timeseries

# 2. Piezo chroniques (~50M rows)
docker exec brgm-dlt-worker dbt run --full-refresh --select stg_piezo_chroniques

# 3. Hydro obs elab (~40M rows) - le modele qui bloquait
docker exec brgm-dlt-worker dbt run --full-refresh --select stg_hydrometry_obs_elab
```

#### 3.3 Test incremental Gold
```bash
# Test un modele Gold pour verifier que le predicat fonctionne
docker exec brgm-dlt-worker dbt run --select int_hydro_daily_measurements

# Verifier dans les logs que le DELETE est rapide (< 1 min)
```

#### 3.4 Tests dbt
```bash
docker exec brgm-dlt-worker dbt test
```
Verifier : 105 PASS, 2 WARN (pre-existant), 0 ERROR.

#### 3.5 Verification tables converties
```sql
-- Verifier que Silver n'est PLUS hypertable
SELECT hypertable_name FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'silver';
-- Attendu : vide (0 rows)

-- Verifier que Gold EST toujours hypertable
SELECT hypertable_name FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'gold';
-- Attendu : int_daily_measurements, int_hydro_daily_measurements,
--           int_era5_for_stations, int_era5_for_hydro_stations,
--           hubeau_daily_chroniques, hydro_daily_chroniques,
--           fct_monthly_chroniques, fct_monthly_hydro
```

---

### Phase 4 : Nettoyage

#### 4.1 Mettre a jour `dbt_project.yml`
Retirer les index bronze sur `hydrometry_obs_elab_raw` si plus necessaires (les index etaient pour accelerer le scan Bronze -> Silver).

#### 4.2 Mettre a jour CLAUDE.md
- Documenter que Silver n'utilise plus TimescaleDB
- Documenter le pattern `incremental_predicates` pour les futurs modeles Gold
- Mettre a jour la table Medallion Layers

#### 4.3 Mettre a jour MEMORY.md
- Ajouter la regle : "Jamais de hypertable sur les tables Silver staging"
- Ajouter : "Toujours utiliser incremental_predicates sur les hypertables Gold avec delete+insert"

---

## 4. Risques et mitigations

| Risque | Impact | Mitigation |
|--------|--------|------------|
| Full-refresh Silver long (~1-2h) | Indisponibilite temporaire | Planifier en fenetre de maintenance |
| Perte compression Silver | +30-50% stockage Silver | Silver est transitoire, Gold compresse |
| `incremental_predicates` non supporte | Build fail | Verifie : supporte dbt-postgres 1.7.0 |
| Reprocess avec date ancienne | PK conflict si predicat trop restrictif | Buffer 4x suffit. Pour reprocess complet, utiliser `--full-refresh` |
| Regression donnees | Donnees incorrectes | Tests dbt (105 tests) valident l'integrite |

---

## 5. Estimation effort

| Phase | Duree dev | Duree execution |
|-------|-----------|-----------------|
| Phase 1 : modifier 3 modeles Silver | 10 min | - |
| Phase 2 : modifier 8 modeles Gold | 15 min | - |
| Phase 3.1 : compile | - | 30 sec |
| Phase 3.2 : full-refresh Silver | - | 1-2h (maintenance) |
| Phase 3.3-3.5 : tests | - | 10 min |
| Phase 4 : docs | 10 min | - |
| **Total** | **~35 min** | **~2h maintenance** |
