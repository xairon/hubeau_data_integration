# Architecture Hub'Eau Pipeline

## Vue d'Ensemble

```
┌─────────────────────────────────────────────────────────────────┐
│                      SOURCES DE DONNÉES                         │
├────────────────────────────────┬────────────────────────────────┤
│         APIs Hub'Eau           │          ERA5 (Copernicus)     │
│  Piézométrie | Hydrométrie     │      Météo France métropole    │
└────────────────────────────────┴────────────────────────────────┘
                    │                            │
                    ▼                            ▼
         ┌──────────────────────────────────────────────────────┐
         │                 DLT (Ingestion)                      │
         │  - Extraction API avec pagination                    │
         │  - Déduplication (MERGE/UPSERT)                      │
         │  - Retry automatique                                 │
         └──────────────────────────────────────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────────────┐
         │            PostgreSQL (Schéma: staging)              │
         │  Tables brutes : *_raw                               │
         └──────────────────────────────────────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────────────┐
         │                 dbt (Transformation)                 │
         │  - Staging: Vues avec typage                         │
         │  - Intermediate: Mapping, agrégation                 │
         │  - Marts: Tables finales                             │
         └──────────────────────────────────────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────────────┐
         │            PostgreSQL (Schéma: hubeau)               │
         │  Tables propres pour l'analyse                       │
         └──────────────────────────────────────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────────────┐
         │              Dagster (Orchestration)                 │
         │  - UI Web (port 49500)                               │
         │  - Jobs pour DLT et dbt                              │
         └──────────────────────────────────────────────────────┘
```

## Composants

### 1. DLT - Ingestion (Bronze)

Extraction des APIs vers PostgreSQL.

- **Mode** : FULL ou Partitionné par année
- **Output** : Tables `staging.*_raw`
- **Config** : `configs/hubeau/*.yml`

### 2. dbt - Transformation (Silver/Gold)

Nettoyage et structuration des données.

**Modèles** :
| Layer | Schéma | Matérialisation | Rôle |
|-------|--------|-----------------|------|
| Staging | `staging` | View | Typage, renommage |
| Intermediate | `hubeau` | Table | Mapping spatial, agrégation |
| Marts | `hubeau` | Table | Tables finales |

**Hooks automatiques** :
- Création d'index sur les tables sources au premier run

### 3. PostgreSQL - Stockage

| Schéma | Contenu |
|--------|---------|
| `staging` | Tables brutes DLT (`*_raw`) |
| `hubeau` | Tables transformées dbt |

### 4. Dagster - Orchestration

| Service | Port | Rôle |
|---------|------|------|
| Webserver | 49500 | UI de monitoring |
| Daemon | - | Exécution jobs/sensors |
| Worker | 4000 | Exécution DLT/dbt |

## Docker Services

```yaml
postgres:          # PostgreSQL 16 + PostGIS
dagster_postgres:  # Base métadonnées Dagster
dlt_worker:        # Worker (DLT + dbt)
dagster_webserver: # UI Dagster
dagster_daemon:    # Scheduler
adminer:           # UI PostgreSQL
```

## Flux de Données

### Ingestion (DLT)
```
Job Dagster → Asset DLT → API Hub'Eau → PostgreSQL staging.*_raw
```

### Transformation (dbt)
```
Job Dagster → dbt build → PostgreSQL hubeau.*
```

---

## Mapping Spatial ERA5 ↔ Stations Piézo

### Principe

Les données ERA5 sont sur une **grille régulière** de 0.1° (~11 km).
Les stations piézométriques sont à des coordonnées précises.

Pour associer chaque station au point de grille ERA5 le plus proche, on arrondit les coordonnées :

```sql
era5_latitude  = ROUND(station_latitude * 10) / 10
era5_longitude = ROUND(station_longitude * 10) / 10
```

### Exemple

| Station | Lat originale | Lon originale | → ERA5 Lat | → ERA5 Lon |
|---------|---------------|---------------|------------|------------|
| BSS001 | 48.723 | 2.598 | 48.7 | 2.6 |
| BSS002 | 48.756 | 2.612 | 48.8 | 2.6 |

### Visualisation

```
      2.5       2.6       2.7
       │         │         │
 48.8 ─┼─────────●─────────┼─  ← Point grille ERA5 (48.8, 2.6)
       │         │  •BSS002│
       │    •BSS001        │
 48.7 ─┼─────────●─────────┼─  ← Point grille ERA5 (48.7, 2.6)
       │         │         │
```

BSS001 (48.723, 2.598) → arrondi → (48.7, 2.6)
BSS002 (48.756, 2.612) → arrondi → (48.8, 2.6)

---

## Tables Principales

### Staging (DLT)
- `piezometry_stations_raw`
- `piezometry_chroniques_raw`
- `era5_france_timeseries`

### Hubeau (dbt)
- `hubeau_daily_chroniques` — Piézométrie + Météo combinées
