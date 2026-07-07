# Ingestion températures journalières CDS (chantier temp, phase ingestion) — Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Ingérer `derived-era5-land-daily-statistics` (t2m mean/min/max journaliers, France, 0.1°) dans une nouvelle voie bronze/silver, prête pour le backfill 1950→présent. Le cutover des marts est HORS de ce plan (gated sur la complétude du backfill).

**Architecture:** Miroir de la voie ERA5 existante (`era5_assets.py`) : nouvelle table bronze hypertable + asset historique partitionné 1 an + smart update quotidien + staging dbt. Trois requêtes CDS par période (une par statistique), fusionnées sur (time, lat, lon) avant insertion.

**Tech Stack:** Dagster, cdsapi, xarray/netCDF4, dbt 1.7.0, TimescaleDB.

## Global Constraints

- **Entrées normatives** : spec `docs/superpowers/specs/2026-07-07-era5-daily-temperature-stats-design.md` ; spike `.superpowers/sdd/spike-cds-daily-stats.md` (dict de requête canonique, `t2m` Kelvin, `valid_time` 1/jour, `number` à dropper, NaN = mer) ; implémentation de référence `src/hubeau_pipeline/assets/bronze/era5_assets.py` (client CADS, retry, DELETE+insert idempotent, hypertable, conversion K→°C, cache-busting year/month/day dérivés de la fenêtre réelle).
- Dataset : `derivated…` NON — exactement `derived-era5-land-daily-statistics` ; `daily_statistic` ∈ {daily_mean, daily_minimum, daily_maximum} ; `time_zone: 'utc+00:00'` ; `frequency: '1_hourly'` ; area [51.5, -5.5, 41.0, 10.0] ; réponse .nc direct (pas de ZIP).
- Table bronze : `bronze.era5_daily_temp_stats(id BIGSERIAL, time timestamp, latitude NUMERIC(6,3), longitude NUMERIC(6,3), t2m_mean NUMERIC(6,2), t2m_min NUMERIC(6,2), t2m_max NUMERIC(6,2), source_file_id TEXT, created_at timestamp DEFAULT now(), PRIMARY KEY (time, id))` + index time / (lat,lon) / source_file — hypertable chunks 1 an, compression segmentby source_file_id après 30 j. Valeurs converties **K→°C** à l'insertion, lignes toutes-NaN (mer) filtrées.
- JAMAIS d'ALTER sur les hypertables existantes ; la nouvelle table est créée en code (pattern `_ensure_table` d'era5_assets.py).
- Lag de disponibilité : **7 jours** (production CDS ~6 j, marge) — env var `ERA5_DAILY_STATS_LAG_DAYS` défaut 7.
- Schedule quotidien 03h30 UTC (`30 3 * * *`), même gating `DAGSTER_ENABLE_SCHEDULES`.
- dbt manuel : `docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt <cmd> --profiles-dir /app/src/dbt_hubeau`.
- Python : ruff clean sur fichiers touchés ; redeploy = `docker compose restart dlt_worker` + reload code location ; commits FR conventionnels + trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task T1 : Source + assets bronze

**Files:**
- Create: `configs/era5/era5_daily_temp_stats.yml` (dataset, variable, statistiques, area, start_year 1950, lag)
- Create: `src/hubeau_pipeline/assets/bronze/era5_daily_temp_assets.py`
- Test: vérifications runtime (pas de pytest — logique I/O ; le smart update sera testé en T3)

**Interfaces:**
- Consumes: patterns d'`era5_assets.py` (client CADS, `ERA5_PARTITIONS_DEF`-like, retry/backoff, DELETE overlap+insert).
- Produces: assets `era5_daily_temp_stats_historical` (StaticPartitionsDefinition 1 an, 1950→année courante, group `era5_historical`) et `era5_daily_temp_stats_update` (smart update, group `era5_daily_stats`), table `bronze.era5_daily_temp_stats` (schéma ci-dessus). Fonction cœur `process_daily_stats_range(start_date, end_date, context)` : pour chaque statistique {mean, min, max} → 1 requête CDS → xarray (drop `number`, `valid_time`→date) → merge des 3 DataFrames sur (time, latitude, longitude) → K→°C arrondi 2 déc. → DELETE fenêtre + INSERT (execute_values). Smart update : même logique de fenêtre qu'`era5_weekly_update` (MAX(time)−2 j, cap 60 j, lag 7 j).

- [ ] Step 1 : écrire le YAML (miroir d'`era5_france_meteo.yml`, champs : dataset, variable 2m_temperature, daily_statistics [daily_mean, daily_minimum, daily_maximum], time_zone, frequency, area, start_year 1950, availability_lag_days 7, retry 3×/10 s)
- [ ] Step 2 : écrire le module assets (≈ miroir era5_assets.py ; réutiliser ses helpers par import si propres, sinon dupliquer localement avec commentaire de provenance)
- [ ] Step 3 : `ruff check` sur le module → clean ; `python3 -m py_compile` OK
- [ ] Step 4 : commit `feat(era5): assets bronze températures journalières CDS (daily mean/min/max, backfill-ready)`

### Task T2 : Staging dbt + tests

**Files:**
- Create: `src/dbt_hubeau/models/staging/stg_era5_daily_temp_stats.sql`
- Modify: `src/dbt_hubeau/models/staging/schema.yml`, `src/dbt_hubeau/models/staging/sources.yml` (source `staging.era5_daily_temp_stats` → nom réel table bronze, comme era5_france_timeseries)

**Interfaces:**
- Consumes: `bronze.era5_daily_temp_stats` (T1).
- Produces: `silver.stg_era5_daily_temp_stats(latitude, longitude, time, t2m_mean, t2m_min, t2m_max, source_file_id, created_at)` — incrémental append, `ROUND(cast,1)` + `DISTINCT ON` (pattern exact de `stg_era5_timeseries.sql`, y compris var de reprocess `era5_daily_temp_reprocess_from_timestamp`), hypertable 1 mois, compression 90 j, PK (lat,lon,time).

- [ ] Step 1 : modèle SQL (copier la structure de stg_era5_timeseries.sql, adapter colonnes)
- [ ] Step 2 : schema.yml : not_null (time, lat, lon, t2m_mean) ; `accepted_range` [-45, 45] warn sur les 3 t2m ; `expression_is_true` `t2m_min <= t2m_mean AND t2m_mean <= t2m_max` (warn) ; `expression_is_true` `latitude*10 = ROUND(latitude*10)` (error)
- [ ] Step 3 : `dbt parse` OK (le run réel attend des données T3)
- [ ] Step 4 : commit `feat(dbt): staging températures journalières ERA5 (stg_era5_daily_temp_stats)`

### Task T3 : Câblage + test bout-en-bout (petite fenêtre réelle)

**Files:**
- Modify: `src/hubeau_pipeline/assets/__init__.py`, `src/hubeau_pipeline/jobs/era5_jobs.py` (+ exports `jobs/__init__.py`), `src/hubeau_pipeline/schedules.py` (03h30 UTC)

**Interfaces:**
- Produces: jobs `era5_daily_temp_historical_load` (partitionné) et `era5_daily_temp_update_job` ; schedule `daily_era5_temp_stats_schedule`. Definitions valides.

- [ ] Step 1 : wiring (imports, all_bronze_assets, all_jobs, all_schedules) — miroir des entrées era5 existantes
- [ ] Step 2 : restart worker + reload code location + `definitions OK`
- [ ] Step 3 : matérialiser `era5_daily_temp_stats_update` (fenêtre auto ≈ 60 derniers jours, 3 requêtes CDS, ~10-20 min de queue) → vérifier bronze : `SELECT COUNT(*), COUNT(DISTINCT (latitude,longitude)), MIN(time), MAX(time), MIN(t2m_min), MAX(t2m_max) FROM bronze.era5_daily_temp_stats;` — attendu ~11 496 cellules × ~53 j, t2m en °C plausibles (−20..45), min ≤ max
- [ ] Step 4 : `dbt run --select stg_era5_daily_temp_stats` + `dbt test --select stg_era5_daily_temp_stats` → PASS/WARN, volumétrie silver = bronze dédupliqué
- [ ] Step 5 : commit `feat(dagster): chaîne températures journalières câblée (jobs, schedule 03h30, staging validé bout-en-bout)`

### Task T4 : Lancement du backfill 1950→présent (fond)

- [ ] Step 1 : lancer le job partitionné via Dagster (backfill de partitions 1950→2025, l'update couvre 2026) — PAS en une fois : par tranches de ~10 partitions pour surveiller les quotas CDS, `dagster/concurrency_key: era5_historical` sérialise déjà
- [ ] Step 2 : vérifier les 2-3 premières partitions (volumétrie ≈ 11 496 × 365 × années, ranges °C plausibles par décennie)
- [ ] Step 3 : consigner dans le ledger la procédure de reprise (relance partition échouée = idempotent) et l'état d'avancement ; la surveillance de complétude long-terme se fera au cutover
- [ ] Step 4 : mise à jour `docs/ERA5.md` (nouvelle voie, statut backfill en cours, cutover à venir) + commit

**Cutover (hors plan, plan dédié quand backfill complet)** : bascule temp de `fct_era5_monthly_grid` (COALESCE), full refresh, rebuild climatologie, re-bootstrap indices, docs/étiquetage junon, `_CHECKS` complétude.
