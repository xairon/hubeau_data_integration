# ERA5 Lots 0 + 1 (pipeline) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Nettoyer le sous-système ERA5 (Lot 0) puis matérialiser dans Gold les marts climat par point de grille + indices SPI/STI (Lot 1), conformément à la spec `docs/superpowers/specs/2026-07-06-era5-climate-module-redesign-design.md`.

**Architecture:** Deux nouveaux modèles dbt marts (`fct_era5_monthly_grid` incrémental delete+insert, `fct_era5_climatology_grid` table full-rebuild) + un module Python pur `ml/era5_indices.py` (scipy) + un asset Dagster `fct_era5_indices_grid` qui upsert une table Gold séparée — même pattern de propriété que l'IPS existant (`fct_monthly_index`).

**Tech Stack:** dbt 1.7.0 (PostgreSQL 16 + TimescaleDB), Dagster, scipy/numpy/pandas, psycopg2.

## Global Constraints

- dbt pinné `>=1.7.0, <1.8.0` (`require-dbt-version` dans `dbt_project.yml`) — ne rien introduire de dbt 1.8+.
- Tables mensuelles = **tables plain** (PAS d'hypertable) avec `delete+insert` + `incremental_predicates` (règle projet).
- JAMAIS `unique_key` sur un modèle `append` (dbt 1.7.0 générerait DELETE...USING). Les deux nouveaux modèles sont `delete+insert` (unique_key OK) ou `table`.
- PK/FK via post-hooks idempotents existants : `add_primary_key([...])`.
- Invocation dbt manuelle dans le worker : `docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt <cmd> --profiles-dir /app/src/dbt_hubeau` (le raccourci CLAUDE.md sans `-w` échoue).
- Tests Python : `PYTHONPATH=src python3 -m pytest tests/ -o addopts=""` (conftest.py stubbe dagster/dlt — les modules `ml/*` ne doivent importer que numpy/pandas/scipy).
- Convention de signe ERA5 : `potential_evaporation` est **négative** (flux descendant, vérifié en base : avg ≈ −9.7 mm/j). ETP positive = `-potential_evaporation`.
- Seuils de classes standardisés (partagés avec l'IPS) : z ∈ ±0.84 / ±1.28 / ±1.75.
- Coordonnées grille : déjà arrondies à 0.1° dans `stg_era5_timeseries` (bug de précision traité en amont — ne PAS re-arrondir en aval).
- Après modification du code Python : `docker compose restart dlt_worker` PUIS reload de la code location (GraphQL `reloadRepositoryLocation` ou UI Dagster). Le restart seul ne suffit pas.
- Commits : messages conventionnels français (`feat(dbt): …`, `chore: …`), terminer par `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: Lot 0 — Purge du code mort ERA5/Pastas

**Files:**
- Delete: `src/hubeau_pipeline/sources/era5_source.py`
- Delete: `tests/test_pastas_assets.py`, `tests/test_pastas_refit.py`, `tests/test_pastas_sgi.py`, `tests/test_pastas_signatures.py`
- Modify: `src/hubeau_pipeline/jobs/era5_jobs.py:5,19` (commentaires « 2 ans »)
- Modify: `src/hubeau_pipeline/assets/bronze/era5_assets.py:382` (commentaire « 2 ans »)
- DB: `DROP TABLE IF EXISTS staging.era5_france_meteo_raw`, `DROP TABLE IF EXISTS gold.int_pastas_station_profile`

**Interfaces:**
- Consumes: rien.
- Produces: rien (suppression pure). ⚠️ Le seed `ref_stations_meteeau_bsn` est **conservé** (consommé par junon `observatory_situation.py:235`) — ne pas y toucher.

- [ ] **Step 1: Vérifier qu'aucun import ne référence les fichiers à supprimer**

Run: `grep -rn "era5_source\|pastas" src/hubeau_pipeline/ --include='*.py' | grep -v "\.pyc"`
Expected: aucune ligne hors de `sources/era5_source.py` lui-même (auto-références). Si une référence externe apparaît, STOP et signaler.

- [ ] **Step 2: Supprimer les fichiers morts**

```bash
git rm src/hubeau_pipeline/sources/era5_source.py \
       tests/test_pastas_assets.py tests/test_pastas_refit.py \
       tests/test_pastas_sgi.py tests/test_pastas_signatures.py
```

- [ ] **Step 3: Corriger les commentaires obsolètes « 2 ans » (partitions = 1 an)**

Dans `src/hubeau_pipeline/jobs/era5_jobs.py`, remplacer :
```python
- Partitionné par chunks de 2 ans (ex: "2024_2025")
```
par :
```python
- Partitionné par chunks de 1 an (ERA5_YEARS_PER_CHUNK, ex: "2024")
```
et dans la description du job :
```python
        "Partitionné par chunks de 2 ans. Télécharge & Insère directement."
```
par :
```python
        "Partitionné par chunks de 1 an. Télécharge & Insère directement."
```
Dans `src/hubeau_pipeline/assets/bronze/era5_assets.py` ligne 382, remplacer `Partitionné par chunks de 2 ans.` par `Partitionné par chunks de 1 an (ERA5_YEARS_PER_CHUNK).`

- [ ] **Step 4: Vérifier que les tests Python passent (collecte réparée)**

Run: `PYTHONPATH=src python3 -m pytest tests/ -o addopts="" -q`
Expected: PASS — plus d'erreur de collecte `ModuleNotFoundError: hubeau_pipeline.ml.pastas_wrapper` ; les tests restants (`test_indices`, `test_monthly_index*`, `test_reference_grid`) passent.

- [ ] **Step 5: Dropper les tables orphelines en base**

```bash
docker exec brgm-postgres psql -U postgres -d postgres \
  -c "DROP TABLE IF EXISTS staging.era5_france_meteo_raw;" \
  -c "DROP TABLE IF EXISTS gold.int_pastas_station_profile;"
```
Expected: `DROP TABLE` ×2 (ou `NOTICE: table does not exist, skipping`).

- [ ] **Step 6: Vérifier que le worker charge toujours les définitions**

Run: `docker exec brgm-dlt-worker python -c "from hubeau_pipeline import defs; print('definitions OK')"`
Expected: `definitions OK`

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "chore(era5): purge code mort (source DLT era5, tests pastas orphelins, commentaires 2 ans)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Lot 0 — `era5_distance_m` dans le mart piézo daily

**Files:**
- Modify: `src/dbt_hubeau/models/marts/hubeau_daily_chroniques.sql:78` (ajout colonne)
- Modify: `src/dbt_hubeau/models/marts/schema.yml` (doc colonne)
- DB: `ALTER TABLE gold.hubeau_daily_chroniques ADD COLUMN era5_distance_m numeric`

**Interfaces:**
- Consumes: `int_station_era5_mapping.era5_distance_m` (existe déjà, mètres géodésiques).
- Produces: colonne `era5_distance_m numeric` dans `gold.hubeau_daily_chroniques` (NULL sur l'historique non recalculé — la distance est constante par station, les consommateurs historiques joignent `int_station_era5_mapping`).

- [ ] **Step 1: Ajouter la colonne au modèle SQL**

Dans `src/dbt_hubeau/models/marts/hubeau_daily_chroniques.sql`, après la ligne `map.era5_longitude::numeric AS era5_longitude` ajouter :
```sql
        map.era5_longitude::numeric AS era5_longitude,
        map.era5_distance_m::numeric AS era5_distance_m
```
(remplace la version sans virgule finale sur `era5_longitude`).

- [ ] **Step 2: ALTER TABLE en base (l'incrémental `append` n'ajoute pas de colonne tout seul)**

```bash
docker exec brgm-postgres psql -U postgres -d postgres \
  -c "ALTER TABLE gold.hubeau_daily_chroniques ADD COLUMN IF NOT EXISTS era5_distance_m numeric;"
```
Expected: `ALTER TABLE`.

- [ ] **Step 3: Documenter la colonne dans schema.yml**

Dans `src/dbt_hubeau/models/marts/schema.yml`, sous les colonnes de `hubeau_daily_chroniques` (après `station_longitude`), ajouter :
```yaml
      - name: era5_distance_m
        description: >
          Distance géodésique station → point de grille ERA5 mappé (mètres).
          NULL sur l'historique antérieur à l'ajout de la colonne (2026-07) : la distance
          est constante par station, joindre int_station_era5_mapping si besoin.
```

- [ ] **Step 4: Builder le modèle et vérifier**

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --profiles-dir /app/src/dbt_hubeau --select hubeau_daily_chroniques
docker exec brgm-postgres psql -U postgres -d postgres \
  -c "SELECT COUNT(*) FILTER (WHERE era5_distance_m IS NOT NULL) AS filled FROM gold.hubeau_daily_chroniques WHERE date >= CURRENT_DATE - INTERVAL '7 days';"
```
Expected: dbt `Completed successfully` ; `filled` > 0 (la fenêtre incrémentale de 30 j vient d'être réécrite avec la distance).

- [ ] **Step 5: Commit**

```bash
git add src/dbt_hubeau/models/marts/hubeau_daily_chroniques.sql src/dbt_hubeau/models/marts/schema.yml
git commit -m "feat(dbt): era5_distance_m dans hubeau_daily_chroniques (symétrie avec le mart hydro)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Lot 0 — Tests dbt manquants sur les colonnes météo

**Files:**
- Modify: `src/dbt_hubeau/models/marts/schema.yml` (PEV daily + agrégats monthly/yearly)
- Modify: `src/dbt_hubeau/models/intermediate/schema.yml` (PEV sur `int_era5_for_all_stations`)

**Interfaces:**
- Consumes: colonnes existantes (`potential_evaporation`, `temperature_moyenne`, `precipitation_totale`, `evaporation_moyenne`, et variantes `_annuelle`).
- Produces: tests `dbt_utils.accepted_range` severity warn. Rappel signe : PEV quotidienne négative, bornes [−100, 5] mm/j ; `evaporation_moyenne` (AVG des PEV) même convention.

- [ ] **Step 1: Ajouter les tests PEV aux deux marts daily**

Dans `src/dbt_hubeau/models/marts/schema.yml`, sous `hubeau_daily_chroniques` (après le bloc `total_precipitation`) ET sous `hydro_daily_chroniques` (même position), ajouter :
```yaml
      - name: potential_evaporation
        description: >
          Évaporation potentielle journalière ERA5 en mm — convention flux descendant :
          valeurs NÉGATIVES (ETP positive = -potential_evaporation). NULL si ERA5 absent.
        tests:
          - dbt_utils.accepted_range:
              min_value: -100
              max_value: 5
              config:
                severity: warn
                where: "potential_evaporation IS NOT NULL"
```

- [ ] **Step 2: Ajouter les tests d'agrégats mensuels**

Dans `src/dbt_hubeau/models/marts/schema.yml`, sous `fct_monthly_chroniques` (après `niveau_moyen`) ET sous `fct_monthly_hydro` (après `resultat_moyen`), ajouter :
```yaml
      - name: temperature_moyenne
        tests:
          - dbt_utils.accepted_range:
              min_value: -30
              max_value: 40
              config:
                severity: warn
                where: "temperature_moyenne IS NOT NULL"
      - name: precipitation_totale
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1500
              config:
                severity: warn
                where: "precipitation_totale IS NOT NULL"
      - name: evaporation_moyenne
        description: "AVG des PEV journalières (mm, convention négative ERA5)"
        tests:
          - dbt_utils.accepted_range:
              min_value: -100
              max_value: 5
              config:
                severity: warn
                where: "evaporation_moyenne IS NOT NULL"
```

- [ ] **Step 3: Ajouter les tests d'agrégats annuels**

Sous `fct_yearly_stats` (après `annee`) ET sous `fct_yearly_hydro` (après `annee`), ajouter :
```yaml
      - name: temperature_moyenne_annuelle
        tests:
          - dbt_utils.accepted_range:
              min_value: -15
              max_value: 30
              config:
                severity: warn
                where: "temperature_moyenne_annuelle IS NOT NULL"
      - name: precipitation_totale_annuelle
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 5000
              config:
                severity: warn
                where: "precipitation_totale_annuelle IS NOT NULL"
```

- [ ] **Step 4: Ajouter le test PEV sur l'intermédiaire**

Dans `src/dbt_hubeau/models/intermediate/schema.yml`, sous `int_era5_for_all_stations` (à côté des tests `temperature_2m`/`total_precipitation` existants), ajouter :
```yaml
      - name: potential_evaporation
        tests:
          - dbt_utils.accepted_range:
              min_value: -100
              max_value: 5
              config:
                severity: warn
                where: "potential_evaporation IS NOT NULL"
```

- [ ] **Step 5: Parser puis exécuter les nouveaux tests**

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt parse --profiles-dir /app/src/dbt_hubeau
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt test --profiles-dir /app/src/dbt_hubeau \
  --select hubeau_daily_chroniques hydro_daily_chroniques fct_monthly_chroniques fct_monthly_hydro fct_yearly_stats fct_yearly_hydro int_era5_for_all_stations
```
Expected: parse OK ; tests PASS ou WARN (warn = valeurs extrêmes à examiner, pas bloquant). Un FAIL (error) = STOP et investiguer.

- [ ] **Step 6: Commit**

```bash
git add src/dbt_hubeau/models/marts/schema.yml src/dbt_hubeau/models/intermediate/schema.yml
git commit -m "test(dbt): ranges sur potential_evaporation et agrégats météo mensuels/annuels

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Lot 1 — Mart mensuel par point de grille `fct_era5_monthly_grid`

**Files:**
- Create: `src/dbt_hubeau/models/marts/fct_era5_monthly_grid.sql`
- Modify: `src/dbt_hubeau/models/marts/schema.yml` (bloc modèle + tests)

**Interfaces:**
- Consumes: `ref('stg_era5_timeseries')` — colonnes `latitude, longitude, time, temperature_2m, total_precipitation, potential_evaporation` (coordonnées déjà à 0.1°).
- Produces: table `gold.fct_era5_monthly_grid`, PK `(era5_latitude, era5_longitude, mois)`. Colonnes : `era5_latitude numeric`, `era5_longitude numeric`, `mois date` (1er du mois), `temperature_moyenne`, `temperature_min`, `temperature_max`, `precipitation_totale`, `etp_totale` (mm **positifs** = SUM(−PEV)), `bilan_hydrique` (= precipitation_totale − etp_totale), `nb_jours int`, `mois_complet boolean`. ~10,5 M lignes.

- [ ] **Step 1: Écrire le modèle**

Créer `src/dbt_hubeau/models/marts/fct_era5_monthly_grid.sql` :
```sql
{{
  config(
    materialized = 'incremental',
    unique_key = ['era5_latitude', 'era5_longitude', 'mois'],
    incremental_strategy = 'delete+insert',
    incremental_predicates = [
      time_range_delete_predicate('mois', '4 months')
    ],
    indexes = [
      {'columns': ['mois'], 'type': 'brin'},
      {'columns': ['era5_latitude', 'era5_longitude']}
    ],
    post_hook = [
      "{{ add_primary_key(['era5_latitude', 'era5_longitude', 'mois']) }}"
    ]
  )
}}

-- Agrégation mensuelle ERA5 par point de grille (0.1°), toute la France, 1950→présent.
-- Base du module Climat junon : vue Situation, séries de point, comparaisons.
-- Signe PEV : ERA5 stocke la PEV négative (flux descendant) → etp_totale = SUM(-pev) (mm positifs).
-- INCREMENTAL delete+insert : régénère les 3 derniers mois (predicate 4 mois > fenêtre
-- régénérée pour couvrir le 1er du mois tronqué — sinon conflit de PK).
-- Table plain (PAS d'hypertable) : règle projet pour les tables mensuelles.

WITH daily AS (
    SELECT
        latitude,
        longitude,
        time::date AS jour,
        temperature_2m,
        total_precipitation,
        potential_evaporation
    FROM {{ ref('stg_era5_timeseries') }}
    {% if is_incremental() %}
    WHERE time >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
    {% endif %}
)

SELECT
    latitude  AS era5_latitude,
    longitude AS era5_longitude,
    DATE_TRUNC('month', jour)::date AS mois,

    AVG(temperature_2m) AS temperature_moyenne,
    MIN(temperature_2m) AS temperature_min,
    MAX(temperature_2m) AS temperature_max,

    SUM(total_precipitation)        AS precipitation_totale,
    SUM(-potential_evaporation)     AS etp_totale,
    SUM(total_precipitation) - SUM(-potential_evaporation) AS bilan_hydrique,

    COUNT(*) AS nb_jours,
    -- Mois complet = autant de jours que le mois calendaire en compte
    COUNT(*) = EXTRACT(DAY FROM (DATE_TRUNC('month', jour) + INTERVAL '1 month - 1 day'))::int
        AS mois_complet

FROM daily
GROUP BY latitude, longitude, DATE_TRUNC('month', jour)
```

- [ ] **Step 2: Ajouter le bloc schema.yml**

Dans `src/dbt_hubeau/models/marts/schema.yml`, ajouter à la fin :
```yaml
  - name: fct_era5_monthly_grid
    description: >
      Agrégats météo mensuels ERA5 par point de grille 0.1° (toute la France, 1950→présent).
      Base du module Climat junon. etp_totale et bilan_hydrique en mm positifs
      (PEV ERA5 négative inversée à l'agrégation).
    columns:
      - name: era5_latitude
        tests: [not_null]
      - name: era5_longitude
        tests: [not_null]
      - name: mois
        tests: [not_null]
      - name: precipitation_totale
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1500
              config:
                severity: warn
      - name: etp_totale
        description: "ETP mensuelle en mm POSITIFS (= SUM(-potential_evaporation))"
        tests:
          - dbt_utils.accepted_range:
              min_value: -50
              max_value: 2500
              config:
                severity: warn
      - name: nb_jours
        tests:
          - dbt_utils.accepted_range:
              min_value: 1
              max_value: 31
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - era5_latitude
            - era5_longitude
            - mois
          config:
            severity: warn
      # Cohérence du bilan hydrique (tolérance flottants)
      - dbt_utils.expression_is_true:
          expression: "ABS(bilan_hydrique - (precipitation_totale - etp_totale)) < 0.01"
          config:
            severity: warn
            where: "bilan_hydrique IS NOT NULL"
```

- [ ] **Step 3: Build initial (full — agrège 321 M lignes, prévoir 15-60 min)**

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --profiles-dir /app/src/dbt_hubeau --select fct_era5_monthly_grid
```
Expected: `Completed successfully`. (Lancer en arrière-plan si nécessaire.)

- [ ] **Step 4: Vérifier volumétrie et fraîcheur**

```bash
docker exec brgm-postgres psql -U postgres -d postgres -c "
SELECT COUNT(*) AS rows, COUNT(DISTINCT (era5_latitude, era5_longitude)) AS cells,
       MIN(mois) AS first, MAX(mois) AS last,
       COUNT(*) FILTER (WHERE NOT mois_complet) AS incomplete
FROM gold.fct_era5_monthly_grid;"
```
Expected: `rows` ≈ 10 000 000–10 600 000 ; `cells` = 11496 ; `first` = 1950-01-01 ; `last` = mois courant ; `incomplete` ≈ 11496×1-2 (mois courant, éventuellement 1950-01 qui démarre au 02/01).

- [ ] **Step 5: Rejouer l'incrémental (idempotence) puis tester**

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --profiles-dir /app/src/dbt_hubeau --select fct_era5_monthly_grid
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt test --profiles-dir /app/src/dbt_hubeau --select fct_era5_monthly_grid
```
Expected: run incrémental rapide (< 2 min, ~3 mois régénérés) sans erreur de PK ; tests PASS/WARN, 0 FAIL. Re-vérifier le COUNT du Step 4 (inchangé → pas de doublons).

- [ ] **Step 6: Commit**

```bash
git add src/dbt_hubeau/models/marts/fct_era5_monthly_grid.sql src/dbt_hubeau/models/marts/schema.yml
git commit -m "feat(dbt): mart mensuel ERA5 par point de grille (fct_era5_monthly_grid)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Lot 1 — Normales 1991-2020 `fct_era5_climatology_grid`

**Files:**
- Create: `src/dbt_hubeau/models/marts/fct_era5_climatology_grid.sql`
- Modify: `src/dbt_hubeau/models/marts/schema.yml`

**Interfaces:**
- Consumes: `ref('fct_era5_monthly_grid')` (mois_complet, precipitation_totale, temperature_moyenne).
- Produces: table `gold.fct_era5_climatology_grid`, PK `(era5_latitude, era5_longitude, mois_calendaire, fenetre)`. Colonnes : `mois_calendaire int` (1-12), `fenetre int` (1/3/6/12), `nb_annees int`, `precip_moyenne`, `precip_stddev`, `prob_zero` (part des cumuls ≤ 0), `gamma_alpha`, `gamma_beta` (méthode des moments sur cumuls > 0 : α = m²/v, β = v/m — β est le paramètre *scale*), `temp_moyenne`, `temp_stddev`. ~552 k lignes. Rebuild complet à chaque run dbt (coût faible).

- [ ] **Step 1: Écrire le modèle**

Créer `src/dbt_hubeau/models/marts/fct_era5_climatology_grid.sql` :
```sql
{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['mois_calendaire', 'fenetre']}
    ],
    post_hook = [
      "{{ add_primary_key(['era5_latitude', 'era5_longitude', 'mois_calendaire', 'fenetre']) }}"
    ]
  )
}}

-- Normales climatiques 1991-2020 par cellule × mois calendaire × fenêtre glissante (1/3/6/12 mois).
-- Fournit les paramètres SPI (gamma, méthode des moments, cf. McKee 1993 avec prob_zero)
-- et STI (moyenne/écart-type) consommés par l'asset Python fct_era5_indices_grid.
-- Fenêtres ROWS BETWEEN : la grille ERA5 est continue mensuellement par cellule (vérifié via n_<w> = <w>).

{% set windows = [1, 3, 6, 12] %}

WITH base AS (
    SELECT
        era5_latitude,
        era5_longitude,
        mois,
        precipitation_totale,
        temperature_moyenne
    FROM {{ ref('fct_era5_monthly_grid') }}
    WHERE mois_complet
      -- 1990 inclus : les fenêtres 12 mois finissant début 1991 en ont besoin
      AND mois >= DATE '1990-01-01'
      AND mois <  DATE '2021-01-01'
),

rolled AS (
    SELECT
        era5_latitude,
        era5_longitude,
        mois,
        {% for w in windows %}
        SUM(precipitation_totale) OVER (
            PARTITION BY era5_latitude, era5_longitude ORDER BY mois
            ROWS BETWEEN {{ w - 1 }} PRECEDING AND CURRENT ROW) AS precip_{{ w }},
        AVG(temperature_moyenne) OVER (
            PARTITION BY era5_latitude, era5_longitude ORDER BY mois
            ROWS BETWEEN {{ w - 1 }} PRECEDING AND CURRENT ROW) AS temp_{{ w }},
        COUNT(*) OVER (
            PARTITION BY era5_latitude, era5_longitude ORDER BY mois
            ROWS BETWEEN {{ w - 1 }} PRECEDING AND CURRENT ROW) AS n_{{ w }}{{ "," if not loop.last }}
        {% endfor %}
    FROM base
),

unpivoted AS (
    {% for w in windows %}
    SELECT
        era5_latitude,
        era5_longitude,
        EXTRACT(MONTH FROM mois)::int AS mois_calendaire,
        {{ w }} AS fenetre,
        precip_{{ w }} AS precip_cumul,
        temp_{{ w }}   AS temp_fenetre
    FROM rolled
    WHERE mois >= DATE '1991-01-01'
      AND n_{{ w }} = {{ w }}
    {{ "UNION ALL" if not loop.last }}
    {% endfor %}
),

stats AS (
    SELECT
        era5_latitude,
        era5_longitude,
        mois_calendaire,
        fenetre,
        COUNT(*)                                            AS nb_annees,
        AVG(precip_cumul)                                   AS precip_moyenne,
        STDDEV_SAMP(precip_cumul)                           AS precip_stddev,
        AVG((precip_cumul <= 0)::int::numeric)              AS prob_zero,
        AVG(precip_cumul)      FILTER (WHERE precip_cumul > 0) AS precip_moy_pos,
        VAR_SAMP(precip_cumul) FILTER (WHERE precip_cumul > 0) AS precip_var_pos,
        AVG(temp_fenetre)                                   AS temp_moyenne,
        STDDEV_SAMP(temp_fenetre)                           AS temp_stddev
    FROM unpivoted
    GROUP BY era5_latitude, era5_longitude, mois_calendaire, fenetre
)

SELECT
    era5_latitude,
    era5_longitude,
    mois_calendaire,
    fenetre,
    nb_annees,
    precip_moyenne,
    precip_stddev,
    prob_zero,
    -- Gamma méthode des moments sur les cumuls > 0 (β = scale)
    CASE WHEN precip_var_pos > 0 THEN precip_moy_pos ^ 2 / precip_var_pos END AS gamma_alpha,
    CASE WHEN precip_moy_pos > 0 AND precip_var_pos > 0
         THEN precip_var_pos / precip_moy_pos END                             AS gamma_beta,
    temp_moyenne,
    temp_stddev
FROM stats
```

- [ ] **Step 2: Ajouter le bloc schema.yml**

Dans `src/dbt_hubeau/models/marts/schema.yml`, ajouter :
```yaml
  - name: fct_era5_climatology_grid
    description: >
      Normales 1991-2020 par cellule ERA5 × mois calendaire × fenêtre (1/3/6/12 mois) :
      paramètres gamma (SPI, méthode des moments, prob_zero) et moyenne/écart-type (STI).
    columns:
      - name: mois_calendaire
        tests:
          - not_null
          - dbt_utils.accepted_range: {min_value: 1, max_value: 12}
      - name: fenetre
        tests:
          - not_null
          - accepted_values:
              values: [1, 3, 6, 12]
              quote: false
      - name: nb_annees
        tests:
          - dbt_utils.accepted_range:
              min_value: 25
              max_value: 30
              config:
                severity: warn
      - name: gamma_alpha
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 10000
              config:
                severity: warn
                where: "gamma_alpha IS NOT NULL"
      - name: gamma_beta
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 10000
              config:
                severity: warn
                where: "gamma_beta IS NOT NULL"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - era5_latitude
            - era5_longitude
            - mois_calendaire
            - fenetre
```

- [ ] **Step 3: Build + tests**

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --profiles-dir /app/src/dbt_hubeau --select fct_era5_climatology_grid
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt test --profiles-dir /app/src/dbt_hubeau --select fct_era5_climatology_grid
```
Expected: run OK (quelques minutes), tests PASS/WARN.

- [ ] **Step 4: Vérifier la volumétrie et une valeur de bon sens**

```bash
docker exec brgm-postgres psql -U postgres -d postgres -c "
SELECT COUNT(*) AS rows, MIN(nb_annees) AS min_years FROM gold.fct_era5_climatology_grid;" -c "
SELECT precip_moyenne, gamma_alpha, gamma_beta, temp_moyenne
FROM gold.fct_era5_climatology_grid
WHERE era5_latitude = 47.4 AND era5_longitude = 0.7 AND mois_calendaire = 6 AND fenetre = 3;"
```
Expected: `rows` = 551 808 (11496 × 12 × 4) ; `min_years` = 30 ; pour Tours (47.4, 0.7) juin fenêtre 3 : precip_moyenne ~ 150-250 mm, temp_moyenne ~ 14-18 °C, alpha/beta > 0.

- [ ] **Step 5: Commit**

```bash
git add src/dbt_hubeau/models/marts/fct_era5_climatology_grid.sql src/dbt_hubeau/models/marts/schema.yml
git commit -m "feat(dbt): normales climatiques 1991-2020 par cellule ERA5 (fct_era5_climatology_grid)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Lot 1 — Module de calcul `ml/era5_indices.py` (TDD)

**Files:**
- Create: `src/hubeau_pipeline/ml/era5_indices.py`
- Test: `tests/test_era5_indices.py`

**Interfaces:**
- Consumes: rien (module pur numpy/pandas/scipy — PAS d'import dagster, le conftest stubbe seulement dagster/dlt).
- Produces:
  - `compute_spi(cumul, gamma_alpha, gamma_beta, prob_zero) -> np.ndarray` — vectorisé ; SPI = Φ⁻¹(q + (1−q)·GammaCDF(x; α, scale=β)), CDF clippée [0.001, 0.999], arrondi 3 décimales, NaN si α/β invalides.
  - `compute_sti(temp, temp_moyenne, temp_stddev) -> np.ndarray` — vectorisé ; STI = (t − μ)/σ, NaN si σ ≤ 0, arrondi 3 décimales.
  - `MIN_YEARS_REF = 25` — seuil WMO : en dessous, l'appelant met les indices à NULL.

- [ ] **Step 1: Écrire les tests (échouent : module absent)**

Créer `tests/test_era5_indices.py` :
```python
"""Golden tests du calcul SPI/STI grille ERA5 (McKee 1993, gamma méthode des moments)."""
import numpy as np
from scipy import stats

from hubeau_pipeline.ml.era5_indices import MIN_YEARS_REF, compute_spi, compute_sti


def test_spi_median_of_gamma_is_near_zero():
    # Le cumul égal à la médiane de la gamma de référence doit donner SPI ≈ 0
    alpha, beta = 4.0, 25.0  # moyenne 100 mm
    median = stats.gamma.ppf(0.5, alpha, scale=beta)
    spi = compute_spi(np.array([median]), np.array([alpha]), np.array([beta]), np.array([0.0]))
    assert abs(spi[0]) < 1e-6


def test_spi_golden_value_exact():
    # Valeur dorée : cumul 50 mm sous gamma(4, scale=25) → cdf ~0.1429 → z ~ -1.0672
    alpha, beta, x = 4.0, 25.0, 50.0
    expected = round(float(stats.norm.ppf(stats.gamma.cdf(x, alpha, scale=beta))), 3)
    spi = compute_spi(np.array([x]), np.array([alpha]), np.array([beta]), np.array([0.0]))
    assert spi[0] == expected


def test_spi_prob_zero_shifts_distribution():
    # Avec q=0.2, H(x) = 0.2 + 0.8*G(x) : un cumul nul doit donner Φ⁻¹(0.2) (clip inclus)
    spi = compute_spi(np.array([0.0]), np.array([4.0]), np.array([25.0]), np.array([0.2]))
    assert spi[0] == round(float(stats.norm.ppf(0.2)), 3)


def test_spi_invalid_params_gives_nan():
    spi = compute_spi(np.array([100.0, 100.0]),
                      np.array([np.nan, -1.0]),
                      np.array([25.0, 25.0]),
                      np.array([0.0, 0.0]))
    assert np.isnan(spi).all()


def test_spi_extreme_clipped_to_ppf_bounds():
    # CDF clippée à [0.001, 0.999] → SPI borné ≈ ±3.09
    spi = compute_spi(np.array([1e6]), np.array([4.0]), np.array([25.0]), np.array([0.0]))
    assert spi[0] == round(float(stats.norm.ppf(0.999)), 3)


def test_sti_basic_zscore():
    sti = compute_sti(np.array([22.0]), np.array([20.0]), np.array([2.0]))
    assert sti[0] == 1.0


def test_sti_zero_sigma_gives_nan():
    sti = compute_sti(np.array([20.0]), np.array([20.0]), np.array([0.0]))
    assert np.isnan(sti[0])


def test_min_years_ref_constant():
    assert MIN_YEARS_REF == 25
```

- [ ] **Step 2: Vérifier qu'ils échouent**

Run: `PYTHONPATH=src python3 -m pytest tests/test_era5_indices.py -o addopts="" -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'hubeau_pipeline.ml.era5_indices'`

- [ ] **Step 3: Implémenter le module**

Créer `src/hubeau_pipeline/ml/era5_indices.py` :
```python
"""SPI/STI grille ERA5 (McKee 1993). Vectorisé numpy/scipy.

SPI : cumul de précipitations → CDF gamma (méthode des moments, paramètres précalculés
dans gold.fct_era5_climatology_grid) mélangée avec la probabilité de cumul nul
(H(x) = q + (1-q)·G(x)) → quantile normal. STI : z-score de la température moyenne
de fenêtre contre la normale 1991-2020. Mêmes seuils de classes que l'IPS (indices.py).
"""
from __future__ import annotations

import numpy as np
from scipy import stats

# Seuil WMO : nombre minimal d'années valides dans la référence pour un indice fiable.
MIN_YEARS_REF = 25

_CDF_CLIP = (0.001, 0.999)


def compute_spi(cumul, gamma_alpha, gamma_beta, prob_zero):
    """SPI vectorisé. NaN si alpha/beta invalides (<=0 ou NaN).

    Args: arrays alignés — cumul (mm), gamma_alpha, gamma_beta (scale), prob_zero [0,1].
    Returns: np.ndarray float64, arrondi à 3 décimales, NaN si non calculable.
    """
    cumul = np.asarray(cumul, dtype=float)
    alpha = np.asarray(gamma_alpha, dtype=float)
    beta = np.asarray(gamma_beta, dtype=float)
    q = np.nan_to_num(np.asarray(prob_zero, dtype=float), nan=0.0)

    valid = np.isfinite(cumul) & np.isfinite(alpha) & np.isfinite(beta) & (alpha > 0) & (beta > 0)
    out = np.full(cumul.shape, np.nan)
    if not valid.any():
        return out

    g = stats.gamma.cdf(np.clip(cumul[valid], 0, None), alpha[valid], scale=beta[valid])
    h = q[valid] + (1.0 - q[valid]) * g
    h = np.clip(h, *_CDF_CLIP)
    out[valid] = np.round(stats.norm.ppf(h), 3)
    return out


def compute_sti(temp, temp_moyenne, temp_stddev):
    """STI vectorisé : (t − μ)/σ. NaN si σ <= 0 ou entrées non finies."""
    t = np.asarray(temp, dtype=float)
    mu = np.asarray(temp_moyenne, dtype=float)
    sigma = np.asarray(temp_stddev, dtype=float)

    valid = np.isfinite(t) & np.isfinite(mu) & np.isfinite(sigma) & (sigma > 0)
    out = np.full(t.shape, np.nan)
    out[valid] = np.round((t[valid] - mu[valid]) / sigma[valid], 3)
    return out
```

- [ ] **Step 4: Vérifier que les tests passent**

Run: `PYTHONPATH=src python3 -m pytest tests/test_era5_indices.py -o addopts="" -q`
Expected: 8 passed.

- [ ] **Step 5: Lint + commit**

```bash
ruff check src/hubeau_pipeline/ml/era5_indices.py tests/test_era5_indices.py
git add src/hubeau_pipeline/ml/era5_indices.py tests/test_era5_indices.py
git commit -m "feat(ml): calcul SPI/STI vectorisé pour la grille ERA5 (golden tests)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Lot 1 — Persistence + asset Dagster `fct_era5_indices_grid` + câblage

**Files:**
- Create: `src/hubeau_pipeline/ml/era5_indices_persistence.py`
- Create: `src/hubeau_pipeline/assets/era5_indices_assets.py`
- Modify: `src/hubeau_pipeline/assets/__init__.py` (import + `all_indices_assets`)
- Modify: `src/hubeau_pipeline/jobs/dbt_jobs.py:41-49` (sélection du job `station_index_refresh`)

**Interfaces:**
- Consumes: `compute_spi` / `compute_sti` / `MIN_YEARS_REF` (Task 6) ; tables `gold.fct_era5_monthly_grid` (Task 4) et `gold.fct_era5_climatology_grid` (Task 5) ; `PostgreSQLResource.get_connection()` (existant, `resources.py`).
- Produces: table `gold.fct_era5_indices_grid` (PK `era5_latitude, era5_longitude, month, fenetre` ; colonnes `spi`, `sti`, `computed_at`) ; asset Dagster nommé `fct_era5_indices_grid` (group `indices`) ajouté au job `station_index_refresh`. Auto-bootstrap : table vide → historique complet par tranches d'années ; sinon 3 derniers mois.

- [ ] **Step 1: Écrire la persistence**

Créer `src/hubeau_pipeline/ml/era5_indices_persistence.py` :
```python
"""Create + upsert gold.fct_era5_indices_grid (SPI/STI par cellule ERA5, fenêtres 1/3/6/12)."""
from psycopg2.extras import execute_values

_CREATE = """
CREATE TABLE IF NOT EXISTS gold.fct_era5_indices_grid (
    era5_latitude  numeric(6,3) NOT NULL,
    era5_longitude numeric(6,3) NOT NULL,
    month          date NOT NULL,
    fenetre        smallint NOT NULL,
    spi            double precision,
    sti            double precision,
    computed_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (era5_latitude, era5_longitude, month, fenetre)
);
CREATE INDEX IF NOT EXISTS idx_fct_era5_indices_grid_month
    ON gold.fct_era5_indices_grid (month, fenetre);
"""

_UPSERT = """
INSERT INTO gold.fct_era5_indices_grid
    (era5_latitude, era5_longitude, month, fenetre, spi, sti, computed_at)
VALUES %s
ON CONFLICT (era5_latitude, era5_longitude, month, fenetre) DO UPDATE SET
    spi = EXCLUDED.spi,
    sti = EXCLUDED.sti,
    computed_at = now();
"""

_TEMPLATE = "(%s, %s, %s, %s, %s, %s, now())"


def init_era5_indices_table(pg):
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cur.execute(_CREATE)
        conn.commit()


def upsert_era5_indices(pg, rows):
    """rows: iterable of (lat, lon, month_date, fenetre, spi|None, sti|None)."""
    if not rows:
        return
    with pg.get_connection() as conn:
        cur = conn.cursor()
        execute_values(cur, _UPSERT, rows, template=_TEMPLATE, page_size=10_000)
        conn.commit()


def latest_index_month(pg):
    """Dernier mois indexé, ou None si la table est vide (déclenche le bootstrap complet)."""
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT MAX(month) FROM gold.fct_era5_indices_grid")
        return cur.fetchone()[0]
```

- [ ] **Step 2: Écrire l'asset**

Créer `src/hubeau_pipeline/assets/era5_indices_assets.py` :
```python
"""SPI/STI par cellule de grille ERA5 → gold.fct_era5_indices_grid.

Nightly (job station_index_refresh, sensor post-transform) : recalcule les 3 derniers mois.
Bootstrap : table vide → historique complet 1950→présent par tranches de 5 ans.
Les paramètres de référence (gamma/μ/σ 1991-2020) viennent de gold.fct_era5_climatology_grid.
"""
import logging

import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset

from ..ml.era5_indices import MIN_YEARS_REF, compute_spi, compute_sti
from ..ml.era5_indices_persistence import (
    init_era5_indices_table,
    latest_index_month,
    upsert_era5_indices,
)
from ..resources import PostgreSQLResource

logger = logging.getLogger(__name__)

WINDOWS = [1, 3, 6, 12]
NIGHTLY_MONTHS = 3        # fenêtre de recalcul quotidienne
BOOTSTRAP_CHUNK_YEARS = 5

# Cumuls/moyennes glissants par cellule, joints aux normales, pour une fenêtre donnée.
# Le warmup de 11 mois avant start_month garantit des fenêtres 12 mois complètes.
_QUERY = """
WITH rolled AS (
    SELECT
        era5_latitude, era5_longitude, mois,
        SUM(precipitation_totale) OVER w AS precip_cumul,
        AVG(temperature_moyenne)  OVER w AS temp_fenetre,
        COUNT(*)                  OVER w AS n_mois
    FROM gold.fct_era5_monthly_grid
    WHERE mois_complet
      AND mois >= %(warmup_month)s
      AND mois <  %(end_month)s
    WINDOW w AS (
        PARTITION BY era5_latitude, era5_longitude ORDER BY mois
        ROWS BETWEEN %(window_minus_1)s PRECEDING AND CURRENT ROW
    )
)
SELECT
    r.era5_latitude, r.era5_longitude, r.mois,
    r.precip_cumul, r.temp_fenetre,
    c.gamma_alpha, c.gamma_beta, c.prob_zero,
    c.temp_moyenne, c.temp_stddev, c.nb_annees
FROM rolled r
JOIN gold.fct_era5_climatology_grid c
  ON c.era5_latitude = r.era5_latitude
 AND c.era5_longitude = r.era5_longitude
 AND c.mois_calendaire = EXTRACT(MONTH FROM r.mois)::int
 AND c.fenetre = %(window)s
WHERE r.mois >= %(start_month)s
  AND r.n_mois = %(window)s
"""


def _compute_range(pg, start_month, end_month):
    """Calcule et upserte SPI/STI pour [start_month, end_month), toutes fenêtres."""
    total = 0
    for window in WINDOWS:
        warmup = start_month - pd.DateOffset(months=window - 1)
        with pg.get_connection() as conn:
            df = pd.read_sql(
                _QUERY,
                conn,
                params={
                    "warmup_month": warmup.date(),
                    "start_month": start_month.date(),
                    "end_month": end_month.date(),
                    "window": window,
                    "window_minus_1": window - 1,
                },
            )
        if df.empty:
            continue
        spi = compute_spi(df["precip_cumul"], df["gamma_alpha"], df["gamma_beta"], df["prob_zero"])
        sti = compute_sti(df["temp_fenetre"], df["temp_moyenne"], df["temp_stddev"])
        # Seuil WMO : référence trop courte → indices NULL
        thin = df["nb_annees"].to_numpy() < MIN_YEARS_REF
        spi[thin] = np.nan
        sti[thin] = np.nan
        rows = [
            (lat, lon, mois, window,
             None if np.isnan(s) else float(s),
             None if np.isnan(t) else float(t))
            for lat, lon, mois, s, t in zip(
                df["era5_latitude"], df["era5_longitude"], df["mois"], spi, sti
            )
        ]
        upsert_era5_indices(pg, rows)
        total += len(rows)
    return total


@asset(
    name="fct_era5_indices_grid",
    group_name="indices",
    deps=[AssetKey("fct_era5_monthly_grid"), AssetKey("fct_era5_climatology_grid")],
    description=(
        "SPI/STI par cellule ERA5 (fenêtres 1/3/6/12 mois, normale 1991-2020). "
        "Nightly: 3 derniers mois. Table vide: bootstrap 1950→présent."
    ),
)
def fct_era5_indices_grid(context: AssetExecutionContext, pg: PostgreSQLResource):
    init_era5_indices_table(pg)

    now_month = pd.Timestamp.today().normalize().replace(day=1)
    last = latest_index_month(pg)

    if last is None:
        context.log.info("Table vide → bootstrap historique complet 1950→présent")
        total = 0
        start = pd.Timestamp("1950-01-01")
        while start < now_month:
            end = min(start + pd.DateOffset(years=BOOTSTRAP_CHUNK_YEARS), now_month)
            n = _compute_range(pg, start, end)
            total += n
            context.log.info("Chunk %s → %s : %d lignes", start.date(), end.date(), n)
            start = end
    else:
        start = now_month - pd.DateOffset(months=NIGHTLY_MONTHS)
        total = _compute_range(pg, start, now_month)
        context.log.info("Recalcul %s → %s : %d lignes", start.date(), now_month.date(), total)

    context.add_output_metadata({"upserted_rows": MetadataValue.int(total)})
    return total
```

- [ ] **Step 3: Câbler l'asset dans `assets/__init__.py`**

Dans `src/hubeau_pipeline/assets/__init__.py` :
```python
from .current_index_assets import station_current_index
from .era5_indices_assets import fct_era5_indices_grid
from .monthly_index_assets import fct_monthly_index
```
et :
```python
all_indices_assets = [
    station_reference_stats,
    station_current_index,
    fct_monthly_index,
    fct_era5_indices_grid,
]
```

- [ ] **Step 4: Ajouter l'asset au job nightly**

Dans `src/hubeau_pipeline/jobs/dbt_jobs.py`, remplacer la définition de `station_current_index_job` :
```python
station_current_index_job = define_asset_job(
    name="station_index_refresh",
    description=(
        "Rebuild gold.fct_monthly_index + gold.station_current_index (IPS/SSFI) + "
        "gold.fct_era5_indices_grid (SPI/STI grille ERA5) after the transform. "
        "Reads the fixed baseline gold.station_reference_stats."
    ),
    selection=AssetSelection.assets(
        "fct_monthly_index", "station_current_index", "fct_era5_indices_grid"
    ),
    tags={"dagster/concurrency_key": "dbt_pipeline"},
)
```

- [ ] **Step 5: Valider les définitions et lint**

```bash
ruff check src/hubeau_pipeline/
docker compose restart dlt_worker && sleep 20
docker exec brgm-dlt-worker python -c "from hubeau_pipeline import defs; print('definitions OK')"
```
Expected: ruff clean ; `definitions OK`.

- [ ] **Step 6: Commit**

```bash
git add src/hubeau_pipeline/ml/era5_indices_persistence.py \
        src/hubeau_pipeline/assets/era5_indices_assets.py \
        src/hubeau_pipeline/assets/__init__.py \
        src/hubeau_pipeline/jobs/dbt_jobs.py
git commit -m "feat(dagster): asset fct_era5_indices_grid (SPI/STI grille) câblé au job station_index_refresh

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: Lot 1 — Bootstrap des indices, vérification bout-en-bout, docs

**Files:**
- Modify: `docs/ERA5.md` (section nouveaux marts)
- Modify: `CLAUDE.md` (comptes de marts + mention des tables climat)

**Interfaces:**
- Consumes: tout ce qui précède (Tasks 4-7 matérialisés).
- Produces: `gold.fct_era5_indices_grid` peuplée 1950→présent (~42 M lignes) ; documentation à jour.

- [ ] **Step 1: Recharger la code location Dagster**

```bash
curl -s -X POST http://localhost:49500/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"mutation { reloadRepositoryLocation(repositoryLocationName: \"hubeau_pipeline\") { __typename } }"}'
```
Expected: réponse JSON sans erreur (`RepositoryLocation` ou `WorkspaceLocationEntry`). Si le nom de location diffère, le lire via `{"query":"{ workspaceOrError { ... on Workspace { locationEntries { name } } } }"}`.

- [ ] **Step 2: Matérialiser l'asset (bootstrap complet — table vide → 1950→présent, prévoir 30-90 min)**

Lancer via l'UI Dagster (asset `fct_era5_indices_grid` → Materialize) ou GraphQL `launchRun`. Suivre les logs : un chunk de 5 ans loggé à la fois.
Expected: run SUCCESS, metadata `upserted_rows` ≈ 40-42 M.

- [ ] **Step 3: Vérifier la table des indices**

```bash
docker exec brgm-postgres psql -U postgres -d postgres -c "
SELECT COUNT(*) AS rows, MIN(month) AS first, MAX(month) AS last,
       COUNT(*) FILTER (WHERE spi IS NULL) AS null_spi
FROM gold.fct_era5_indices_grid;" -c "
SELECT fenetre, ROUND(AVG(spi)::numeric, 3) AS avg_spi, ROUND(STDDEV(spi)::numeric, 3) AS sd_spi
FROM gold.fct_era5_indices_grid
WHERE month BETWEEN '1991-01-01' AND '2020-12-01'
GROUP BY fenetre ORDER BY fenetre;"
```
Expected: `rows` ≈ 40-42 M ; `first` = 1950-01-01 (fenêtre 1 ; les fenêtres longues démarrent plus tard) ; `last` = dernier mois complet ; sur la période de référence 1991-2020, `avg_spi` ≈ 0 (±0.05) et `sd_spi` ≈ 1 (±0.1) pour chaque fenêtre — sanité statistique du SPI.

- [ ] **Step 4: Rejouer l'asset (mode nightly) et vérifier l'idempotence**

Matérialiser à nouveau `fct_era5_indices_grid` (UI/GraphQL).
Expected: run SUCCESS rapide (< 5 min), `upserted_rows` ≈ 11496 × 3 × 4 ≈ 138 k, COUNT total inchangé (upsert).

- [ ] **Step 5: Vérifier la chaîne dbt complète (les 2 nouveaux modèles inclus)**

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt build --profiles-dir /app/src/dbt_hubeau \
  --select fct_era5_monthly_grid+ fct_era5_climatology_grid
```
Expected: `Completed successfully` — modèles + tests OK dans l'ordre du DAG.

- [ ] **Step 6: Mettre à jour la documentation**

Dans `docs/ERA5.md`, ajouter une section :
```markdown
## Marts climat par point de grille (module Climat junon)

- `gold.fct_era5_monthly_grid` — agrégats mensuels par cellule 0.1° (1950→présent, ~10,5 M lignes).
  `etp_totale`/`bilan_hydrique` en mm POSITIFS (PEV ERA5 négative inversée à l'agrégation).
- `gold.fct_era5_climatology_grid` — normales 1991-2020 (gamma MoM + μ/σ) par cellule × mois × fenêtre.
- `gold.fct_era5_indices_grid` — SPI/STI (fenêtres 1/3/6/12) calculés par l'asset Python
  `fct_era5_indices_grid` (job `station_index_refresh`, nightly). Table vide → bootstrap complet.
```
Dans `CLAUDE.md` : section « Medallion Layers » — passer les marts de 10 à 12 ; section « Data Domains / Climate » — mentionner les 3 tables climat grille ; section dbt — noter que `fct_era5_indices_grid` est un asset Python (pas dbt) rattaché à `station_index_refresh`.

- [ ] **Step 7: Commit final**

```bash
git add docs/ERA5.md CLAUDE.md docs/superpowers/specs/2026-07-06-era5-climate-module-redesign-design.md
git commit -m "docs(era5): marts climat grille + indices SPI/STI (Lot 1) — docs et spec à jour

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```
