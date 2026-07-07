# Températures journalières vraies (CDS daily statistics) — Design

**Date** : 2026-07-07
**Statut** : validé sur le principe par l'utilisateur (option « b », 2026-07-07) ; détails au mieux
**Contexte** : suite du Lot 1 (cf. `2026-07-06-era5-climate-module-redesign-design.md`)

## Problème

L'ingestion ERA5 actuelle ne prend qu'**un timestep par jour à 00:00 UTC** (`configs/era5/era5_france_meteo.yml`,
`time: "00:00"`). Parfait pour les cumuls (précipitations, PEV : la valeur 00:00 = cumul complet de
la veille), mais la température est un **instantané nocturne** : biais froid systématique ~2-4 °C,
et les min/max mensuels sont des min/max de minuits, pas des Tn/Tx. Le STI (anomalie) est valide ;
les valeurs absolues affichées ne le sont pas.

## Solution

Ingérer les **statistiques journalières officielles** du dataset CDS
**`derived-era5-land-daily-statistics`** (moyenne/min/max journalières de `2m_temperature`,
calculées par Copernicus sur les 24 timesteps horaires), backfill 1950→présent, puis **cutover**
de la source température des marts grille.

Les cumuls (précip, PEV) **ne changent pas** — la voie actuelle est correcte.

## Architecture (pas d'ALTER sur les hypertables existantes — piège connu)

```
CDS derived-era5-land-daily-statistics (t2m mean/min/max, NetCDF)
  → bronze.era5_daily_temp_stats   (NOUVELLE table, hypertable 1 an compressée)
  → silver.stg_era5_daily_temp_stats (staging : arrondi 0.1°, dédup, hypertable)
  → cutover : fct_era5_monthly_grid lit la température depuis la nouvelle table
    (COALESCE avec l'ancienne voie tant que le backfill est partiel)
```

- **Nouvelle table bronze** `era5_daily_temp_stats(time, latitude, longitude, t2m_mean, t2m_min,
  t2m_max, source_file_id, created_at)` — mêmes conventions que `era5_france_timeseries`
  (PK (time,id), index lat/lon, hypertable 1 an, compression >30 j, K→°C à l'insertion).
- **Assets Dagster** : même pattern que `era5_assets.py` — `era5_daily_temp_stats_historical`
  (partitions 1 an, 1950→présent) + `era5_daily_temp_stats_update` (smart update quotidien,
  lag 5 j, fenêtre glissante) + schedule quotidien 03h30 UTC.
  ⚠️ Le dataset expose `daily_statistic` (une statistique par requête) → 3 requêtes par période
  (mean, min, max), fusionnées avant insertion. Les paramètres exacts de l'API seront validés
  par une requête d'essai (1 mois) avant le backfill.
- **Staging dbt** `stg_era5_daily_temp_stats` : cast + `ROUND(…, 1)` + `DISTINCT ON` (leçon du
  bug de précision), incrémental append, hypertable 1 mois.
- **Backfill** : job partitionné 1950→2026 lancé en tâche de fond (quotas CDS : compter des
  jours/semaines ; ~76 partitions × 3 requêtes). Restartable par partition (idempotent
  DELETE+insert par fenêtre, comme l'historique actuel).

## Cutover (quand le backfill est complet)

1. `fct_era5_monthly_grid` : `temperature_moyenne/min/max` basculent sur
   `AVG(t2m_mean)/MIN(t2m_min)/MAX(t2m_max)` de la nouvelle voie (sémantique enfin exacte :
   min/max = extrêmes des Tn/Tx journalières). Tant que la couverture est partielle :
   `COALESCE(nouvelle voie, ancienne voie)` + colonne `temp_source` pour l'observabilité.
2. Full refresh du mart (≈1 h) → `fct_era5_climatology_grid` se reconstruit au run suivant →
   **re-bootstrap** de `fct_era5_indices_grid` (TRUNCATE + matérialisation, 36 min) car les
   normales de température changent.
3. Mise à jour des descriptions schema.yml/docs (suppression des avertissements 00:00 UTC côté
   grille) et de l'étiquetage junon.
4. `data_completeness_job` : ajouter la nouvelle table bronze aux contrôles.

## Hors périmètre (décisions explicites)

- **Marts station daily** (`hubeau_daily_chroniques`, `hydro_daily_chroniques`) : gardent la
  température 00:00 UTC pour l'instant (changer la source = full refresh de tables hypertables
  lourdes + `int_era5_for_all_stations` 90 M lignes ; à faire dans un second temps si besoin).
- Vent/humidité/neige : toujours hors périmètre.
- L'ancienne colonne `temperature_2m` de `bronze.era5_france_timeseries` continue d'être ingérée
  (elle alimente les marts station) — pas de suppression.

## Tests

- dbt : not_null/ranges sur t2m_mean/min/max (`accepted_range` [-45, 45] warn), cohérence
  `t2m_min <= t2m_mean <= t2m_max` (expression_is_true), arrondi 0.1° (expression_is_true
  `latitude*10 = ROUND(latitude*10)`).
- Sanité post-backfill : Tours normale juin fenêtre 3 doit remonter à ~15-16 °C (vs 12 °C
  aujourd'hui) ; comparaison avant/après sur quelques villes.

## Séquencement

Ce chantier démarre MAINTENANT (le backfill est le chemin critique) mais le **Lot 2 junon
n'attend pas le cutover** : l'UI se construit sur les données actuelles (STI valide), le
cutover améliorera les valeurs absolues sans changement d'API.
