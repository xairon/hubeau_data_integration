# Stockage ERA5

Architecture et gestion des données météorologiques ERA5.

## Vue d'Ensemble

Les données ERA5 sont stockées en **deux formats** dans PostgreSQL :

1. **Fichiers NetCDF bruts** : `bronze.era5_france_meteo_raw` (archivage)
2. **Time series extraites** : `bronze.era5_france_timeseries` (analyse)

## Architecture

```
ERA5 API (Copernicus CDS)
    │
    ▼
era5_meteo_job (DLT)
    │
    ▼
bronze.era5_france_meteo_raw (NetCDF bruts)
    │
    ▼
era5_timeseries_job (Extraction)
    │
    ▼
bronze.era5_france_timeseries (Time series)
    │
    ▼
dbt staging → silver.stg_era5_timeseries
    │
    ▼
dbt intermediate → gold.int_era5_for_stations
    │
    ▼
gold.hubeau_daily_chroniques (Table finale)
```

## Table 1 : `bronze.era5_france_meteo_raw`

**Rôle** : Archivage des fichiers NetCDF bruts téléchargés depuis Copernicus CDS.

### Structure

| Colonne | Type | Description |
|---------|------|-------------|
| `file_id` | TEXT | Identifiant unique (ex: "era5_france_2024_2025") |
| `variables` | JSON | Liste des variables dans le NetCDF |
| `start_year` | INTEGER | Année de début |
| `end_year` | INTEGER | Année de fin |
| `area` | JSON | Bounding box [North, West, South, East] |
| `netcdf_data` | BYTEA | **Fichier NetCDF brut (50-100 MB)** |
| `file_size_mb` | NUMERIC | Taille du fichier en MB |
| `download_timestamp` | TIMESTAMP | Date de téléchargement |

### Statistiques

| Métrique | Valeur |
|----------|--------|
| **Total fichiers** | ~38 fichiers (1950-2025, chunks de 2 ans) |
| **Taille par fichier** | 50-100 MB |
| **Stockage total** | ~3-4 GB |
| **Variables** | 3 (temperature, precipitation, evaporation) |
| **Timesteps** | ~730 par fichier (2 ans × 365 jours) |
| **Points de grille** | ~10,000 (grille 0.1° sur France) |

### ⚠️ Important : Ne Pas Ouvrir dans Adminer

**Problème** : Les fichiers NetCDF sont stockés en BYTEA (50-100 MB chacun). Ouvrir cette table dans Adminer peut :
- ❌ Faire planter le navigateur (chargement de 3+ GB en mémoire)
- ❌ Causer des timeouts
- ❌ Consommer toute la mémoire

**Solution** : Utiliser uniquement la table `bronze.era5_france_timeseries` pour l'analyse.

## Table 2 : `bronze.era5_france_timeseries`

**Rôle** : Time series extraites des fichiers NetCDF, prêtes pour l'analyse SQL.

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
| `source_file_id` | TEXT | Fichier source (ex: "era5_france_2024_2025") |
| `created_at` | TIMESTAMP | Date de création |

### Statistiques

| Métrique | Valeur |
|----------|--------|
| **Total lignes** | ~300M (1950-2025) |
| **Stockage** | ~30-40 GB |
| **Résolution temporelle** | Quotidienne (1 timestep/jour) |
| **Résolution spatiale** | 0.1° (~11 km) |
| **Couverture** | France métropole |

### Index

```sql
CREATE INDEX idx_era5_time ON bronze.era5_france_timeseries (time);
CREATE INDEX idx_era5_location ON bronze.era5_france_timeseries (latitude, longitude);
CREATE INDEX idx_era5_time_location ON bronze.era5_france_timeseries (time, latitude, longitude);
CREATE INDEX idx_era5_source_file ON bronze.era5_france_timeseries (source_file_id);
```

## Extraction des Time Series

### Job : `era5_timeseries_job`

**Rôle** : Extraire les time series depuis les fichiers NetCDF bruts.

**Processus** :
1. Lit les fichiers NetCDF depuis `bronze.era5_france_meteo_raw`
2. Extrait les données avec `xarray`
3. Convertit en DataFrame pandas
4. Insère dans `bronze.era5_france_timeseries`

**Idempotence** : Vérifie si un fichier a déjà été traité avant extraction.

### Exécution

```bash
# Dans Dagster UI
1. Aller dans "Jobs"
2. Sélectionner "era5_timeseries_job"
3. Cliquer sur "Launch Run"
```

**Durée** : ~5-10 minutes par fichier (selon la taille).

## Utilisation

### Requêtes SQL Directes

```sql
-- Température moyenne par jour pour un point de grille
SELECT 
    DATE(time) AS date,
    AVG(temperature_2m) AS temp_moyenne_celsius
FROM bronze.era5_france_timeseries
WHERE latitude = 48.7 
  AND longitude = 2.6
  AND time >= '2024-01-01'
GROUP BY DATE(time)
ORDER BY date;

-- Précipitations totales par mois
SELECT 
    DATE_TRUNC('month', time) AS mois,
    SUM(total_precipitation) AS precip_totale_mm
FROM bronze.era5_france_timeseries
WHERE time >= '2024-01-01'
GROUP BY DATE_TRUNC('month', time)
ORDER BY mois;
```

### Via Table Finale Gold

```sql
-- Données combinées piézo + météo
SELECT 
    code_bss,
    date,
    niveau_nappe_eau,
    temperature_2m,
    total_precipitation,
    potential_evaporation
FROM gold.hubeau_daily_chroniques
WHERE code_bss = 'BSS001XX0001'
  AND date >= '2024-01-01'
ORDER BY date;
```

## Mapping Spatial

Les stations piézométriques sont mappées aux points de grille ERA5 les plus proches :

```sql
-- Algorithme de mapping
era5_latitude  = ROUND(station_latitude * 10) / 10
era5_longitude = ROUND(station_longitude * 10) / 10
```

**Exemple** :
- Station à (48.723, 2.598) → Point grille ERA5 (48.7, 2.6)
- Station à (48.756, 2.612) → Point grille ERA5 (48.8, 2.6)

## Maintenance

### Vérifier les Fichiers Téléchargés

```sql
SELECT 
    file_id,
    start_year,
    end_year,
    file_size_mb,
    download_timestamp
FROM bronze.era5_france_meteo_raw
ORDER BY start_year;
```

### Vérifier les Time Series Extraites

```sql
SELECT 
    source_file_id,
    COUNT(*) AS nb_lignes,
    MIN(time) AS date_debut,
    MAX(time) AS date_fin
FROM bronze.era5_france_timeseries
GROUP BY source_file_id
ORDER BY source_file_id;
```

### Nettoyer les Données Anciennes

```sql
-- Supprimer les time series d'un fichier spécifique
DELETE FROM bronze.era5_france_timeseries
WHERE source_file_id = 'era5_france_2020_2021';

-- Relancer l'extraction
-- (via era5_timeseries_job dans Dagster UI)
```

## Performance

### Optimisations

### 1. **Index sur (latitude, longitude, time)** : Recherche spatiale et temporelle rapide
2. **Filtrage précoce** : `int_era5_for_stations` ne garde que les points de grille utilisés
3. **Agrégation** : Agrégation quotidienne dans `int_daily_measurements`
4. **TimescaleDB** : `stg_era5_timeseries` et `int_era5_for_stations` sont des **Hypertables**, optimisant drastiquement les requêtes par plage de date.

### Volumes

| Table | Lignes | Taille |
|-------|--------|--------|
| `era5_france_meteo_raw` | ~38 | ~3-4 GB |
| `era5_france_timeseries` | ~300M | ~30-40 GB |
| `int_era5_for_stations` | ~10-15M | ~1-2 GB |

## FAQ

### Q: Pourquoi deux tables ?

**A** : 
- `era5_france_meteo_raw` : Archivage des fichiers originaux (reproductibilité)
- `era5_france_timeseries` : Format normalisé pour l'analyse SQL

### Q: Puis-je supprimer `era5_france_meteo_raw` ?

**A** : Oui, si vous avez validé que toutes les time series sont extraites. Mais recommandé de garder pour archivage.

### Q: Comment accéder aux fichiers NetCDF originaux ?

**A** : Via Python/psql :

```python
import psycopg2
import io
import xarray as xr

conn = psycopg2.connect("postgresql://postgres:password@localhost:49502/postgres")
cur = conn.cursor()

cur.execute("SELECT netcdf_data FROM bronze.era5_france_meteo_raw WHERE file_id = 'era5_france_2024_2025'")
netcdf_bytes = cur.fetchone()[0]

ds = xr.open_dataset(io.BytesIO(netcdf_bytes))
print(ds)
```

### Q: Comment ajouter de nouvelles années ?

**A** :
1. Lancer `era5_meteo_job` avec la partition correspondante (ex: `2026_2027`)
2. Lancer `era5_timeseries_job` pour extraire les time series
3. Relancer `dbt_silver_gold_pipeline_job` pour mettre à jour gold
