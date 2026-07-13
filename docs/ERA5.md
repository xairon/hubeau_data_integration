# Stockage ERA5

**Architecture : Direct-to-Timeseries**

Le pipeline n'archive pas les fichiers NetCDF en base. Les données ERA5-Land (résolution
native ~0.1°, servies par le Copernicus Climate Data Store) sont téléchargées, traitées en
mémoire / disque temporaire, puis insérées directement dans la table partitionnée
`bronze.era5_france_timeseries`.

## Vue d'ensemble

```
Copernicus CDS — ERA5-Land (~0.1°)
    │
    ▼
[Job Dagster : era5_meteo_job (historique) / era5_weekly_job (incrémental)]
    │ 1. Téléchargement NetCDF (tmp)
    │ 2. Extraction xarray (en mémoire)
    │ 3. Insertion par lots
    ▼
bronze.era5_france_timeseries (séries temporelles)
    │
    ▼
Couches dbt Silver / Gold
```

## Table : `bronze.era5_france_timeseries`

**Rôle** : Unique source de vérité pour les données météo historiques et récentes.

### Structure

| Colonne | Type | Description |
|---------|------|-------------|
| `id` | BIGSERIAL | Clé primaire |
| `time` | TIMESTAMP | Date/heure (00:00 UTC) |
| `latitude` | NUMERIC(6,3) | Latitude (grille 0.1°) |
| `longitude` | NUMERIC(6,3) | Longitude (grille 0.1°) |
| `temperature_2m` | NUMERIC(6,2) | Température à 2m (°C) |
| `total_precipitation` | NUMERIC(8,4) | Précipitations totales (mm) |
| `potential_evaporation` | NUMERIC(8,4) | Évaporation potentielle (mm) |
| `source_file_id` | TEXT | Traceabilité (ex: "era5_hist_2024_2025") |

### Optimisations
1. **Index spatio-temporels** : `idx_era5_time`, `idx_era5_location`.
2. **TimescaleDB** : table convertie en **hypertable** (chunk d'1 an) + **compression** des chunks anciens.

---

## Pipelines d'Ingestion

### 1. Job historique (`era5_meteo_job`)
- **But** : charger le backlog (1950 → présent), partitionné par blocs d'années.
- **Action** : télécharge le NetCDF, extrait, insère, supprime le fichier temporaire.
- **Idempotence** : supprime la plage existante (DELETE de l'overlap), puis réinsère.
- **Usage** : bootstrap et rattrapage manuel d'une période.

### 2. Job incrémental quotidien (`era5_weekly_job`)
- **But** : mettre à jour les données récentes (jusqu'à `aujourd'hui − ERA5_AVAILABILITY_LAG_DAYS`).
- **Logique « Smart Update »** :
    1. Vérifie la date max en base (`MAX(time)`).
    2. Dérive la fenêtre réelle manquante et ne télécharge que ce delta.
    3. Déclenché chaque jour à 3h00 UTC (schedule).

---

## Mapping spatial
Les stations (piézométriques et hydrométriques) sont rattachées au point de grille ERA5 le plus proche via **PostGIS KNN** (opérateur `<->`), dans `int_station_era5_mapping` et `int_hydro_station_era5_mapping`.

```sql
-- Algorithme de mapping (Nearest Neighbor)
SELECT ...
FROM stations s
CROSS JOIN LATERAL (
    SELECT latitude, longitude
    FROM era5_grid e
    ORDER BY s.geom <-> e.geom
    LIMIT 1
) e
```

**Résultat** : Chaque station est liée à son point météo le plus pertinent géographiquement.

---

## Marts climat par point de grille (module Climat junon)

- `gold.fct_era5_monthly_grid` — agrégats mensuels par cellule 0.1° (1950→présent, ~10,5 M lignes).
  `etp_totale`/`bilan_hydrique` en mm POSITIFS (PEV ERA5 négative inversée à l'agrégation).
  `temperature_moyenne`/`temperature_min`/`temperature_max` = moyenne/min/max des vraies
  statistiques journalières (source `stg_era5_daily_temp_stats`, cf. section suivante),
  agrégées côté CDS à partir des 24 pas horaires du jour. **Cutover 2026-07-13** : ces
  3 colonnes ne dérivent plus de l'échantillon instantané 00:00 UTC de
  `bronze.era5_france_timeseries` — le biais froid nocturne (~2-4°C) décrit plus bas ne
  s'applique plus aux marts grille. Précipitation/ETP/bilan_hydrique restent dérivés de
  `stg_era5_timeseries` (pas de source journalière vraie disponible pour ces variables).
- `gold.fct_era5_climatology_grid` — normales 1991-2020 (gamma MoM + μ/σ) par cellule × mois × fenêtre.
- `gold.fct_era5_indices_grid` — SPI/STI (fenêtres 1/3/6/12) calculés par l'asset Python
  `fct_era5_indices_grid` (job `station_index_refresh`, nightly). Table vide → bootstrap complet.

---

## Voie complémentaire : statistiques journalières (mean/min/max)

**Dataset CDS distinct** : `derived-era5-land-daily-statistics` (vs `reanalysis-era5-land`
pour la timeseries ci-dessus). Calcule côté CADS, à partir des 24 pas horaires, la
moyenne/min/max journalières réelles de `2m_temperature` — sans le biais d'échantillonnage
00:00 UTC. C'est désormais la source de `fct_era5_monthly_grid.temperature_*` (cf. section
précédente).

- **Table bronze** : `bronze.era5_daily_temp_stats(time, latitude, longitude, t2m_mean,
  t2m_min, t2m_max, source_file_id, created_at)` — hypertable chunks 1 an, compression
  après 30 j, K→°C converti à l'insertion.
- **Silver** : `silver.stg_era5_daily_temp_stats` (append incrémental, dédup DISTINCT ON,
  arrondi 1 décimale), tests not_null/accepted_range/expression_is_true (`min≤mean≤max`).
- **Jobs Dagster** : `era5_daily_temp_historical_load` (partitionné 1 an, clés
  `"YYYY_YYYY"`, ex. `1950_1950`) pour le backfill 1950→présent ; `era5_daily_temp_update_job`
  (smart update quotidien, schedule 03h30 UTC).
- **Statut (2026-07-07)** : backfill **EN COURS** — 1ère tranche 1950-1959 lancée via
  `launchPartitionBackfill` (backfill id `keaocyyd`, 10 runs, `dagster/concurrency_key:
  era5_historical` sérialisé par le `QueuedRunCoordinator`). Rythme observé : la queue CDS
  de ce dataset dérivé prend ~2 h par requête (3 requêtes/partition) → ~6 h/partition.
  Tranches suivantes (1960-1969 … 2020-2025) à lancer une fois la tranche courante verte — voir
  `.superpowers/sdd/progress.md` pour la procédure de reprise.
- **Cutover mart (2026-07-13)** : `fct_era5_monthly_grid.temperature_*` dérive désormais de
  `stg_era5_daily_temp_stats` (LEFT JOIN sur lat/lon/jour, pas de COALESCE de repli — un mois
  hors backfill donne `temperature_*` NULL plutôt qu'une valeur biaisée 00:00 UTC).
  Précipitation/ETP/bilan_hydrique/nb_jours/mois_complet restent dérivés de
  `stg_era5_timeseries`, inchangés. **Reste à faire** une fois le backfill 1950→présent
  complet et audité : `dbt run --full-refresh --select fct_era5_monthly_grid`, rebuild
  climatologie + indices SPI/STI en aval, ré-étiquetage junon.

---

## Maintenance

### Vérifier les Time Series
```sql
SELECT 
    source_file_id,
    COUNT(*) AS nb_lignes,
    MIN(time) AS date_debut,
    MAX(time) AS date_fin
FROM bronze.era5_france_timeseries
GROUP BY source_file_id
ORDER BY source_file_id DESC;
```

### Relancer une période
Depuis l'interface Dagster :
1. Job `era5_meteo_job`
2. Sélectionner la partition correspondant à la période à recharger
3. Lancer le run (le job supprime l'overlap puis réinsère ; un DELETE manuel est conseillé si les données sont corrompues).
