# Stockage ERA5

**Architecture Unifiée : Direct-to-Timeseries**

Contrairement aux versions précédentes, le pipeline actuel n'archive plus les fichiers NetCDF en base (`raw`) pour économiser du stockage (50GB+ économisés). Les données sont téléchargées, traitées en mémoire/disque temporaire, et insérées directement dans la table partitionnée `timeseries`.

## Vue d'Ensemble

```
ERA5 API (Copernicus CDS)
    │
    ▼
[Job Dagster: era5_historical_load / era5_weekly_update_job]
    │ 1. Téléchargement NetCDF (tmp)
    │ 2. Extraction xarray (In-Memory)
    │ 3. Insertion par Batch
    ▼
bronze.era5_france_timeseries (Time series)
    │
    ▼
dbt Silver/Gold Layers
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
1.  **Index Spatio-Temporels** : `idx_era5_time`, `idx_era5_location`.
2.  **Partitionnement (Logique)** : Ingestion par chunks de 2 ans.
3.  **TimescaleDB** : Table convertie en **Hypertable** (Chunk `1 year`) + **Compression** activée.

---

## Pipelines d'Ingestion

### 1. Job Historique (`era5_historical_load`)
- **But** : Charger le backlog (1950 - Présent).
- **Méthode** : Partitionné par blocs de 2 ans (ex: 2020-2021).
- **Action** : Télécharge le NetCDF, extrait, insère, supprime le fichier.
- **Idempotence** : Supprime la plage existante (DELETE overlap), puis réinsère.

### 2. Job Hebdomadaire (`era5_weekly_update_job`)
- **But** : Mettre à jour les données récentes (J-5 à aujourd'hui).
- **Logique "Smart Update"** :
    1. Vérifie la date max en base (`MAX(time)`).
    2. Ne télécharge que le delta manquant (+ buffer sécurité).
    3. Si "trou" trop grand (> 60 jours), demande un backfill manuel.

---

## Mapping Spatial
Les stations piézométriques sont mappées aux points de grille ERA5 les plus proches via **PostGIS KNN** (opérateur `<->`).

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
1. Job `era5_historical_load`
2. Sélectionner la partition (ex: `2024_2025`)
3. Lancer le run (le job écrasera ou ignorera selon la logique d'idempotence, delete manuel conseillé si données corrompues).
