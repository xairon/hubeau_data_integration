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
